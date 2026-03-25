from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = 'change-me'
DEBUG = True
ALLOWED_HOSTS = ["innovation.dxteriz.com", "localhost", "127.0.0.1", "139.177.182.162"]

CSRF_TRUSTED_ORIGINS = [
    "https://innovation.dxteriz.com",
    "http://localhost",
    "http://127.0.0.1",
    "http://139.177.182.162",
]

USE_X_FORWARDED_HOST = True
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True

BASE_PATH = os.getenv("BASE_PATH", "/quartier_hors_tension")


def _norm_base(path: str) -> str:
    if not path or path == "/":
        return ""
    return path.strip("/")


_BASE = _norm_base(BASE_PATH)

STATIC_URL = f"/{_BASE}/static/" if _BASE else "/static/"
MEDIA_URL = f"/{_BASE}/media/" if _BASE else "/media/"

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'quartier',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'myproject.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'myproject.wsgi.application'
ASGI_APPLICATION = 'myproject.asgi.application'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}

LANGUAGE_CODE = 'fr-fr'
TIME_ZONE = 'Africa/Abidjan'
USE_I18N = True
USE_TZ = True

STATIC_ROOT = BASE_DIR / 'staticfiles'
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

DATA_DIR = BASE_DIR / 'data'
SHAPEF_DIR = DATA_DIR / 'shapef'

POSTES_XLS = DATA_DIR / 'Poste_HTA_BT_DRAN.xls'
QUARTIER_XLSX = DATA_DIR / 'quartier.xlsx'
POI_PROPOSE_XLSX = DATA_DIR / 'POI_propose.xlsx'
PHARMACIES_GEOJSON = DATA_DIR / 'pharmacies_abj.geojson'

PRECALC_XLSX = DATA_DIR / 'final_postes.xlsx'
FINAL_GEOJSON = DATA_DIR / 'final_postes.geojson'
PRECISION_OVERRIDES_XLSX = DATA_DIR / 'precision_overrides.xlsx'

LANDUSE_SHP = SHAPEF_DIR / 'gis_osm_landuse_a_free_1.shp'
POIS_SHP = SHAPEF_DIR / 'gis_osm_pois_free_1.shp'
ROADS_SHP = SHAPEF_DIR / 'gis_osm_roads_free_1.shp'

CALC_CRS = 'EPSG:32630'
MAP_CRS = 'EPSG:4326'
OSM_SOURCE_CRS = 'EPSG:4326'

DEFAULT_RADIUS = 300
MIN_ZONE_AREA_M2 = 50
