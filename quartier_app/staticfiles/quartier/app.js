(() => {
  const cfg = window.QUARTIER_APP;

  const state = {
    selected: [],
    postesLayer: null,
    quartiersLayer: null,
  };

  const searchInput = document.getElementById("searchInput");
  const searchResults = document.getElementById("searchResults");
  const rayonInput = document.getElementById("rayonInput");
  const computeBtn = document.getElementById("computeBtn");
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
    return `${String(item.libelle).trim()}||${String(item.Nom_poste).trim()}`;
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
        <span><strong>${item.Nom_poste}</strong> · ${item.libelle}</span>
        <button type="button" data-key="${selectedKey(item)}">×</button>
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
    if (!results.length) return;

    const panel = document.createElement("div");
    panel.className = "search-results-panel";

    results.forEach((item) => {
      const row = document.createElement("div");
      row.className = "search-item";
      row.innerHTML = `
        <div class="search-title">${item.Nom_poste}</div>
        <div class="search-meta">Libellé: ${item.libelle} · ${item.DR || "-"} · ${item.EXPLOITATION || "-"}</div>
      `;
      row.addEventListener("click", () => {
        const key = selectedKey(item);
        const exists = state.selected.some((s) => selectedKey(s) === key);
        if (!exists) {
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
    const url = new URL(cfg.searchUrl, window.location.origin);
    url.searchParams.set("q", q);

    const res = await fetch(url);
    const data = await res.json();

    if (!data.ok) {
      console.error(data);
      statusText.textContent = "Erreur recherche";
      return;
    }

    renderSearchResults(data.results || []);
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
    state.selected.forEach((item) => {
      url.searchParams.append("selected", selectedKey(item));
    });
    return url;
  }

  function clearLayers() {
    if (state.postesLayer) map.removeLayer(state.postesLayer);
    if (state.quartiersLayer) map.removeLayer(state.quartiersLayer);
    state.postesLayer = null;
    state.quartiersLayer = null;
  }
function renderMap(postesGeojson, quartiersGeojson) {
  clearLayers();

  const emptyGeojson = { type: "FeatureCollection", features: [] };

  const safeQuartiers = quartiersGeojson || emptyGeojson;
  const safePostes = postesGeojson || emptyGeojson;

  state.quartiersLayer = L.geoJSON(safeQuartiers, {
    style: () => ({
      color: "#1f7a4d",
      weight: 2,
      fillColor: "#79c794",
      fillOpacity: 0.35,
    }),
    onEachFeature: (feature, layer) => {
      const p = feature.properties || {};
      layer.bindPopup(`<strong>${p.quartier || "Quartier"}</strong>`);
    },
  }).addTo(map);

  state.postesLayer = L.geoJSON(safePostes, {
    pointToLayer: (feature, latlng) =>
      L.circleMarker(latlng, {
        radius: 10,
        color: "#ffffff",      // contour blanc
        weight: 3,
        fillColor: "#ff7f00",  // orange vif
        fillOpacity: 1,
      }),
    onEachFeature: (feature, layer) => {
      const p = feature.properties || {};

      layer.bindPopup(`
        <div style="min-width:200px">
          <strong>${p.Nom_poste || "Poste"}</strong><br>
          Rue : ${p.rue || "-"}<br>
          Quartier : ${p.quartier_geo || "-"}<br>
          Ville : ${p.ville || "-"}<br>
          <b>Libellé :</b> ${p.libelle || "-"}
        </div>
      `);

      // layer.bindTooltip(
      //   `${p.Nom_poste || "Poste"}`,
      //   {
      //     permanent: true,
      //     direction: "top",
      //     offset: [0, -12],
      //     opacity: 0.95,
      //     className: "poste-label"
      //   }
      // );
    },
  }).addTo(map);

  // =========================
  // ZOOM PRIORITAIRE SUR LES POSTES
  // =========================
  const posteLayers = state.postesLayer.getLayers();

  if (posteLayers.length === 1) {
    const latlng = posteLayers[0].getLatLng();

    // zoom fort sur un seul poste
    map.setView(latlng, 18, {
      animate: true
    });

    return;
  }

  if (posteLayers.length > 1) {
    const postesBounds = state.postesLayer.getBounds();

    if (postesBounds.isValid()) {
      map.fitBounds(postesBounds, {
        padding: [40, 40],
        maxZoom: 18
      });
      return;
    }
  }

  // fallback si aucun poste mais quartiers présents
  const quartierBounds = state.quartiersLayer.getBounds();
  if (quartierBounds.isValid()) {
    map.fitBounds(quartierBounds, {
      padding: [20, 20],
      maxZoom: 16
    });
  }
}

  function renderTable(rows) {
    resultTbody.innerHTML = "";
    resultCount.textContent = String(rows.length);

    if (!rows.length) {
      resultTbody.innerHTML = '<tr><td colspan="7">Aucun quartier trouvé pour cette sélection.</td></tr>';
      return;
    }

    rows.forEach((row) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${row.libelle ?? ""}</td>
        <td>${row.Nom_poste ?? ""}</td>
        <td>${row.quartier ?? ""}</td>
        <td>${row.commune ?? ""}</td>
        <td>${row.DR ?? ""}</td>
        <td>${row.EXPLOITATION ?? ""}</td>
        <td>${row.distance_m ?? ""}</td>
      `;
      resultTbody.appendChild(tr);
    });
  }

  async function compute() {
    statusText.textContent = "Calcul en cours...";
    try {
      const res = await fetch(buildComputeUrl());
      const data = await res.json();

      if (!data.ok) {
        console.error("API compute error:", data);
        statusText.textContent = "Erreur calcul";
        resultTbody.innerHTML = `<tr><td colspan="7">${data.error || "Erreur inconnue"}</td></tr>`;
        return;
      }

      renderTable(data.rows || []);
      renderMap(data.postes_geojson, data.quartiers_geojson);
      statusText.textContent = `Rayon ${data.rayon} m`;
    } catch (err) {
      console.error(err);
      statusText.textContent = "Erreur JS/API";
      resultTbody.innerHTML = `<tr><td colspan="7">${err}</td></tr>`;
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

  computeBtn.addEventListener("click", compute);
  rayonInput.addEventListener("change", compute);
  downloadBtn.addEventListener("click", () => {
    window.location.href = buildDownloadUrl().toString();
  });

  document.addEventListener("click", (e) => {
    if (!searchResults.contains(e.target) && e.target !== searchInput) {
      searchResults.innerHTML = "";
    }
  });

  renderSelected();
  compute();
})();