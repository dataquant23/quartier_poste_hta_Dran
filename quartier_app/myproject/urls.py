
from django.contrib import admin
from django.urls import path,include
from django.conf import settings
from django.conf.urls.static import static

def _norm_base(path: str) -> str:
    if not path or path == "/":
        return ""
    return path.strip("/")

_BASE = _norm_base(getattr(settings, "BASE_PATH", "/"))
_PREFIX = f"{_BASE}/" if _BASE else ""
urlpatterns = [
    path(f"{_PREFIX}admin/", admin.site.urls),
    path(f"{_PREFIX}", include("quartier.urls")),  
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
