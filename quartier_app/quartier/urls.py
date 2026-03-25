from django.urls import path
from . import views

app_name = 'quartier'

urlpatterns = [
    path('', views.index, name='index'),
    path('api/search-postes/', views.api_search_postes, name='api_search_postes'),
    path('api/compute/', views.api_compute, name='api_compute'),
    path('api/refresh/', views.api_refresh, name='api_refresh'),
    path('api/poste-context/', views.api_poste_context, name='api_poste_context'),
    path('api/update-precision/', views.api_update_precision, name='api_update_precision'),
    path('download/', views.download_excel, name='download_excel'),
]
