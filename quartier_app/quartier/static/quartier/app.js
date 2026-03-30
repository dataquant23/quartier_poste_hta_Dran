(() => {
  const cfg = window.QUARTIER_APP;

  const state = {
    selected: [],
    postesLayer: null,
    zonesLayer: null,
    poisLayer: null,
    pharmaciesLayer: null,
    isRefreshing: false,
    isComputing: false,
  };

  const searchInput = document.getElementById("searchInput");
  const searchResults = document.getElementById("searchResults");
  const rayonInput = document.getElementById("rayonInput");
  const computeBtn = document.getElementById("computeBtn");
  const refreshBtn = document.getElementById("refreshBtn");
  const downloadBtn = document.getElementById("downloadBtn");
  const selectedList = document.getElementById("selectedList");
  const selectionCount = document.getElementById("selectionCount");
  const resultCount = document.getElementById("resultCount");
  const resultTbody = document.querySelector("#resultTable tbody");
  const statusText = document.getElementById("statusText");

  const map = L.map("map").setView([5.34, -4.02], 11);

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap contributors",
  }).addTo(map);

  function selectedKey(item) {
    return `${String(item.libelle ?? "").trim()}||${String(item.Nom_poste ?? "").trim()}`;
  }

  function setStatus(msg) {
    statusText.textContent = msg || "Prête";
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function getCookie(name) {
    const cookieValue = document.cookie
      .split("; ")
      .find((row) => row.startsWith(`${name}=`));
    return cookieValue ? decodeURIComponent(cookieValue.split("=")[1]) : "";
  }

  function renderSelected() {
    selectedList.innerHTML = "";
    selectionCount.textContent = String(state.selected.length);

    if (!state.selected.length) {
      selectedList.innerHTML = '<span class="muted">Aucun poste sélectionné.</span>';
      return;
    }

    state.selected.forEach((item) => {
      const chip = document.createElement("div");
      chip.className = "selected-chip";
      chip.innerHTML = `
        <span><strong>${escapeHtml(item.Nom_poste)}</strong> · ${escapeHtml(item.libelle)}</span>
        <button type="button" data-key="${escapeHtml(selectedKey(item))}">×</button>
      `;
      selectedList.appendChild(chip);
    });

    selectedList.querySelectorAll("button").forEach((btn) => {
      btn.addEventListener("click", () => {
        const key = btn.dataset.key;
        state.selected = state.selected.filter((item) => selectedKey(item) !== key);
        renderSelected();
        compute();
      });
    });
  }

  function renderSearchResults(results) {
    searchResults.innerHTML = "";
    if (!Array.isArray(results) || !results.length) return;

    const panel = document.createElement("div");
    panel.className = "search-results-panel";

    results.forEach((item) => {
      const row = document.createElement("div");
      row.className = "search-item";
      row.innerHTML = `
        <div class="search-title">${escapeHtml(item.Nom_poste)}</div>
        <div class="search-meta">
          Libellé: ${escapeHtml(item.libelle)} · ${escapeHtml(item.DR || "-")} · ${escapeHtml(item.EXPLOITATION || "-")}
        </div>
      `;

      row.addEventListener("click", () => {
        const key = selectedKey(item);
        if (!state.selected.some((s) => selectedKey(s) === key)) {
          state.selected.push(item);
          renderSelected();
          compute();
        }
        searchResults.innerHTML = "";
        searchInput.value = "";
      });

      panel.appendChild(row);
    });

    searchResults.appendChild(panel);
  }

  async function fetchSearch(q) {
    try {
      const url = new URL(cfg.searchUrl, window.location.origin);
      url.searchParams.set("q", q);

      const res = await fetch(url, { credentials: "same-origin" });
      const data = await res.json();
      renderSearchResults(data.results || []);
    } catch (err) {
      console.error("Erreur recherche :", err);
      searchResults.innerHTML = "";
    }
  }

  function buildComputeUrl() {
    const url = new URL(cfg.computeUrl, window.location.origin);
    url.searchParams.set("rayon", rayonInput.value || "300");
    state.selected.forEach((item) => {
      url.searchParams.append("selected", selectedKey(item));
    });
    return url;
  }

  function buildDownloadUrl() {
    const url = new URL(cfg.downloadUrl, window.location.origin);
    url.searchParams.set("rayon", rayonInput.value || "300");
    return url;
  }

  function clearLayers() {
    if (state.postesLayer) map.removeLayer(state.postesLayer);
    if (state.zonesLayer) map.removeLayer(state.zonesLayer);
    if (state.poisLayer) map.removeLayer(state.poisLayer);
    if (state.pharmaciesLayer) map.removeLayer(state.pharmaciesLayer);

    state.postesLayer = null;
    state.zonesLayer = null;
    state.poisLayer = null;
    state.pharmaciesLayer = null;
  }

  function buildContextPopup(p, geo = {}) {
    const lines = [
      ["Rue", geo.rue],
      ["Quartier", geo.quartier_geo],
      ["Voisinage", geo.voisinage],
      ["Nom", geo.name],
      ["Village", geo.village],
      ["Ville", geo.city],
      ["Région", geo.state],
    ]
      .filter(([, value]) => value)
      .map(([label, value]) => `<div><b>${escapeHtml(label)}</b> : ${escapeHtml(value)}</div>`)
      .join("");

    return `
      <div style="min-width:220px">
        <div><strong>${escapeHtml(p.Nom_poste || "Poste")}</strong></div>
        <div><b>Libellé</b> : ${escapeHtml(p.libelle || "-")}</div>
        ${lines || '<div class="muted">Aucune info Nominatim.</div>'}
      </div>
    `;
  }

  async function fetchPosteContext(key) {
    try {
      const url = new URL(cfg.posteContextUrl, window.location.origin);
      url.searchParams.set("selected_key", key);

      const res = await fetch(url, { credentials: "same-origin" });
      const data = await res.json();
      return data.data || {};
    } catch (err) {
      console.error("Erreur contexte poste :", err);
      return {};
    }
  }

  function renderMap(postesGeojson, zonesGeojson, poisGeojson, pharmaciesGeojson) {
    clearLayers();

    const emptyGeojson = { type: "FeatureCollection", features: [] };

    state.zonesLayer = L.geoJSON(zonesGeojson || emptyGeojson, {
      style: () => ({
        color: "#1f7a4d",
        weight: 2,
        fillColor: "#79c794",
        fillOpacity: 0.025,
      }),
      onEachFeature: (feature, layer) => {
        const p = feature.properties || {};

        layer.bindPopup(`
          <div style="min-width:220px">
            <div><strong>Quartier concerné</strong></div>
            <div><b>Quartier</b> : ${escapeHtml(p.quartier_source || "-")}</div>
            <div><b>Poste</b> : ${escapeHtml(p.Nom_poste || "-")}</div>
            <div><b>Libellé</b> : ${escapeHtml(p.libelle || "-")}</div>
          </div>
        `);

        layer.on("mouseover", () => {
          layer.setStyle({
            weight: 3,
            fillOpacity: 0.025,
          });
        });

        layer.on("mouseout", () => {
          state.zonesLayer?.resetStyle(layer);
        });
      },
    }).addTo(map);

    state.poisLayer = L.geoJSON(poisGeojson || emptyGeojson, {
      pointToLayer: (_, latlng) =>
        L.circleMarker(latlng, {
          radius: 5,
          color: "#ffffff",
          weight: 2,
          fillColor: "#2563eb",
          fillOpacity: 1,
        }),
      onEachFeature: (feature, layer) => {
        const p = feature.properties || {};
        layer.bindPopup(`
          <div style="min-width:180px">
            <div><strong>Point proche</strong></div>
            <div>${escapeHtml(p.poi_proche || "-")}</div>
          </div>
        `);
      },
    }).addTo(map);

    state.pharmaciesLayer = L.geoJSON(pharmaciesGeojson || emptyGeojson, {
      pointToLayer: (_, latlng) =>
        L.circleMarker(latlng, {
          radius: 6,
          color: "#ffffff",
          weight: 2,
          fillColor: "#dc2626",
          fillOpacity: 1,
        }),
      onEachFeature: (feature, layer) => {
        const p = feature.properties || {};
        layer.bindPopup(`
          <div style="min-width:180px">
            <div><strong>Pharmacie proche</strong></div>
            <div>${escapeHtml(p.pharmacie || "-")}</div>
          </div>
        `);
      },
    }).addTo(map);

    state.postesLayer = L.geoJSON(postesGeojson || emptyGeojson, {
      pointToLayer: (feature, latlng) =>
        L.circleMarker(latlng, {
          radius: 10,
          color: "#ffffff",
          weight: 3,
          fillColor: "#ff7f00",
          fillOpacity: 1,
        }),
      onEachFeature: (feature, layer) => {
        const p = feature.properties || {};

        layer.bindPopup(
          `<div><strong>${escapeHtml(p.Nom_poste || "Poste")}</strong><br>Chargement...</div>`
        );

        layer.on("click", async () => {
          layer.setPopupContent(
            `<div><strong>${escapeHtml(p.Nom_poste || "Poste")}</strong><br>Chargement Nominatim...</div>`
          );

          const data = await fetchPosteContext(p.selected_key || "");
          layer.setPopupContent(buildContextPopup(data, data.geo_info || {}));
        });
      },
    }).addTo(map);

    const posteLayers = state.postesLayer.getLayers();
    if (posteLayers.length === 1) {
      map.setView(posteLayers[0].getLatLng(), 18, { animate: true });
      return;
    }

    if (posteLayers.length > 1) {
      const bounds = state.postesLayer.getBounds();
      if (bounds.isValid()) {
        map.fitBounds(bounds, { padding: [40, 40], maxZoom: 18 });
        return;
      }
    }

    const zoneBounds = state.zonesLayer.getBounds();
    if (zoneBounds.isValid()) {
      map.fitBounds(zoneBounds, { padding: [20, 20], maxZoom: 16 });
    }
  }

  function buildQuartierDetailsText(row) {
    const details = Array.isArray(row.details) ? row.details : [];

    if (!details.length) {
      return `Quartier : ${row.quartier_source || "-"}

Aucun détail disponible.`;
    }

    const lines = details.map((detail, index) => {
      const parts = [];
      if (detail.poi_proche) parts.push(`POI: ${detail.poi_proche}`);
      if (detail.pharmacie) parts.push(`Pharmacie: ${detail.pharmacie}`);
      if (detail.precision) parts.push(`Précision: ${detail.precision}`);
      return `${index + 1}. ${parts.join(" | ") || "Aucun détail"}`;
    });

    return `Quartier : ${row.quartier_source || "-"}

${lines.join("\n")}`;
  }

  function autoResizeTextareas(container) {
    const textareas = container.querySelectorAll(".precision-inline-input");

    textareas.forEach((textarea) => {
      const resize = () => {
        textarea.style.height = "auto";
        textarea.style.height = `${textarea.scrollHeight}px`;
      };

      resize();
      textarea.addEventListener("input", resize);
    });
  }

  function renderRowDisplay(tr, row) {
    tr.innerHTML = `
      <td>${escapeHtml(row.libelle ?? "")}</td>
      <td>${escapeHtml(row.Nom_poste ?? "")}</td>
      <td>
        <button type="button" class="quartier-tag" title="Voir le détail du quartier">
          ${escapeHtml(row.quartier_label ?? row.quartier_source ?? "")}
        </button>
      </td>
      <td class="precision-cell">
        ${escapeHtml(row.precision ?? "") || '<span class="muted">-</span>'}
      </td>
      <td class="action-cell">
        <button type="button" class="btn btn-secondary btn-edit-inline">Modifier</button>
      </td>
    `;

    tr.querySelector(".quartier-tag")?.addEventListener("click", () => {
      window.alert(buildQuartierDetailsText(row));
    });

    tr.querySelector(".btn-edit-inline")?.addEventListener("click", () => {
      openInlineEditor(tr, row);
    });
  }

  function openInlineEditor(tr, row) {
    tr.innerHTML = `
      <td>${escapeHtml(row.libelle ?? "")}</td>
      <td>${escapeHtml(row.Nom_poste ?? "")}</td>
      <td>
        <button type="button" class="quartier-tag" title="Voir le détail du quartier">
          ${escapeHtml(row.quartier_label ?? row.quartier_source ?? "")}
        </button>
      </td>
      <td class="precision-cell">
        <div class="inline-editor">
          <div class="inline-editor-row">
            <textarea
              class="precision-inline-input"
              data-group-key="${escapeHtml(row.group_key || "")}"
              placeholder="Précision"
            >${escapeHtml(row.precision || "")}</textarea>
          </div>
        </div>
      </td>
      <td class="action-cell">
        <div class="inline-action-group">
          <button type="button" class="btn btn-primary btn-save-inline">Enregistrer</button>
          <button type="button" class="btn btn-light btn-cancel-inline">Annuler</button>
        </div>
      </td>
    `;

    autoResizeTextareas(tr);

    tr.querySelector(".quartier-tag")?.addEventListener("click", () => {
      window.alert(buildQuartierDetailsText(row));
    });

    tr.querySelector(".btn-cancel-inline")?.addEventListener("click", () => {
      renderRowDisplay(tr, row);
      setStatus("Modification annulée");
    });

    tr.querySelector(".btn-save-inline")?.addEventListener("click", async () => {
      try {
        const saveBtn = tr.querySelector(".btn-save-inline");
        const cancelBtn = tr.querySelector(".btn-cancel-inline");
        const input = tr.querySelector(".precision-inline-input");

        if (saveBtn) saveBtn.disabled = true;
        if (cancelBtn) cancelBtn.disabled = true;

        if (!input) {
          setStatus("Aucune valeur à enregistrer");
          if (saveBtn) saveBtn.disabled = false;
          if (cancelBtn) cancelBtn.disabled = false;
          return;
        }

        const precision = input.value.trim();
        const ok = await updatePrecision(row, precision, false);

        if (!ok) {
          if (saveBtn) saveBtn.disabled = false;
          if (cancelBtn) cancelBtn.disabled = false;
          return;
        }

        const updatedRow = {
          ...row,
          precision,
        };

        renderRowDisplay(tr, updatedRow);
        setStatus("Précision enregistrée");
      } catch (err) {
        console.error("Erreur bouton enregistrer :", err);
        setStatus("Erreur pendant l'enregistrement");
      }
    });
  }

  function renderTable(rows) {
    resultTbody.innerHTML = "";
    resultCount.textContent = String(rows.length);

    if (!Array.isArray(rows) || !rows.length) {
      resultTbody.innerHTML = '<tr><td colspan="5">Aucun résultat.</td></tr>';
      return;
    }

    rows.forEach((row) => {
      const tr = document.createElement("tr");
      renderRowDisplay(tr, row);
      resultTbody.appendChild(tr);
    });
  }

  async function updatePrecision(row, precision, refreshAfter = true) {
    try {
      const csrfToken = getCookie("csrftoken");

      const res = await fetch(cfg.updatePrecisionUrl, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          "X-Requested-With": "XMLHttpRequest",
          "X-CSRFToken": csrfToken,
        },
        body: JSON.stringify({
          group_key: row.group_key || "",
          selected_key: row.selected_key || "",
          quartier_source: row.quartier_source || "",
          precision,
        }),
      });

      const contentType = res.headers.get("content-type") || "";
      let data = null;

      if (contentType.includes("application/json")) {
        data = await res.json();
      } else {
        const text = await res.text();
        console.error("Réponse non JSON :", text);
        setStatus(`Erreur serveur (${res.status})`);
        alert(`Erreur serveur (${res.status})`);
        return false;
      }

      if (!res.ok || !data.ok) {
        console.error("Erreur mise à jour :", data);
        setStatus(data?.error || `Erreur mise à jour (${res.status})`);
        alert(data?.error || `Erreur mise à jour (${res.status})`);
        return false;
      }

      if (refreshAfter) {
        await compute();
      }

      return true;
    } catch (err) {
      console.error("Erreur updatePrecision :", err);
      setStatus("Erreur réseau lors de l'enregistrement");
      alert("Erreur réseau lors de l'enregistrement");
      return false;
    }
  }

  async function compute() {
    if (state.isComputing) return;
    state.isComputing = true;

    try {
      if (!state.selected.length) {
        renderTable([]);
        renderMap(null, null, null, null);
        setStatus("Aucun poste sélectionné");
        return;
      }

      setStatus("Lecture du fichier final...");

      const res = await fetch(buildComputeUrl(), {
        credentials: "same-origin",
      });
      const data = await res.json();

      renderTable(data.rows || []);
      renderMap(
        data.postes_geojson,
        data.zones_geojson,
        data.pois_geojson,
        data.pharmacies_geojson
      );
      setStatus(`Rayon ${data.rayon} m`);
    } catch (err) {
      console.error("Erreur compute :", err);
      setStatus("Erreur de chargement");
    } finally {
      state.isComputing = false;
    }
  }

  async function refreshAll() {
    if (state.isRefreshing) return;

    state.isRefreshing = true;
    if (refreshBtn) refreshBtn.disabled = true;

    try {
      setStatus("Rafraîchissement en cours...");

      const url = new URL(cfg.refreshUrl, window.location.origin);
      url.searchParams.set("rayon", rayonInput.value || "300");

      const res = await fetch(url, {
        credentials: "same-origin",
      });
      const data = await res.json();

      if (!data.ok) {
        setStatus(data.error || "Erreur de rafraîchissement");
        return;
      }

      setStatus(`Fichier final régénéré (${data.rows} lignes)`);
      await compute();
    } catch (err) {
      console.error("Erreur refresh :", err);
      setStatus("Erreur de rafraîchissement");
    } finally {
      state.isRefreshing = false;
      if (refreshBtn) refreshBtn.disabled = false;
    }
  }

  let timer = null;

  searchInput.addEventListener("input", () => {
    const q = searchInput.value.trim();
    clearTimeout(timer);

    if (!q) {
      searchResults.innerHTML = "";
      return;
    }

    timer = setTimeout(() => fetchSearch(q), 250);
  });

  computeBtn?.addEventListener("click", compute);
  refreshBtn?.addEventListener("click", refreshAll);
  downloadBtn?.addEventListener("click", () => {
    window.location.href = buildDownloadUrl().toString();
  });

  document.addEventListener("click", (e) => {
    if (!searchResults.contains(e.target) && e.target !== searchInput) {
      searchResults.innerHTML = "";
    }
  });

  renderSelected();
  setStatus("Prête");
})();