# dashboard/urls.py
from django.urls import path
from . import views

app_name = "dashboard"

urlpatterns = [
    path('tl_dashboard/', views.tl_dashboard, name='tl_dashboard'),
    path('api/tl_dashboard_filters/', views.tl_dashboard_filters, name='tl_dashboard_filters'),
    path('api/tl_dashboard_data/', views.tl_dashboard_data, name='tl_dashboard_data'),
]