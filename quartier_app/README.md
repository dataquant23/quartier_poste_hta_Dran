# Application Django Quartier

## Fonctionnalités
- préfixe d'application: `/quartier`
- recherche par poste ou libellé
- calcul dynamique des quartiers selon le rayon
- affichage carte Leaflet
- téléchargement Excel du résultat courant

## Fichiers source à placer dans `data/`
- `quartier.xlsx`
- `Poste_HTA_BT_DRAN.xls`
- `poste_quartiers_300m.xlsx`

## Installation
```bash
pip install -r requirements.txt
python manage.py migrate
python manage.py collectstatic --noinput
python manage.py runserver
```

## URL
- http://127.0.0.1:8000/quartier/

## Nginx (exemple)
```nginx
location /quartier/ {
    proxy_pass http://127.0.0.1:8000/quartier/;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
}

location /quartier/static/ {
    alias /chemin/vers/projet/staticfiles/;
}
```
