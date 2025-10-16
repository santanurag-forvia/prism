# projects/urls.py
from django.urls import path
from . import views

app_name = "projects"

urlpatterns = [
    path("list/", views.project_list, name="list"),
    path("create/", views.create_project, name="create"),

    # Keep the existing named route that accepts a project id (used by reverse('projects:edit', args=[id]))
    path("edit/<int:project_id>/", views.edit_project, name="edit"),

    # New convenience route without project id.
    # The view will select a sensible default (first editable project) when no id is provided.
    path("edit/", views.edit_project, name="edit_default"),

    path("delete/<int:project_id>/", views.delete_project, name="delete"),

    # COE & Domain management (raw-sql)
    path("coes/create/", views.create_coe, name="coes_create"),
    path("coes/edit/<int:coe_id>/", views.edit_coe, name="coes_edit"),
    path("domains/create/", views.create_domain, name="domains_create"),
    path("domains/edit/<int:domain_id>/", views.edit_domain, name="domains_edit"),

    # AJAX endpoints
    path("ldap-search/", views.ldap_search, name="ldap_search"),

    # New endpoints required by the mapping UI and live-refresh:
    path("map-coes/", views.map_coes, name="map_coes"),
    path("api/coes/", views.api_coes, name="api_coes"),
    path("api/projects/", views.api_projects, name="api_projects"),
    path("api/subprojects/", views.api_subprojects, name="api_subprojects"),
    path("team-allocations/", views.team_allocations, name="team_allocations"),
    path("team-allocations/save/", views.save_team_allocation, name="save_team_allocation"),

    path("my-allocations/update-status/", views.my_allocations_update_status, name="my_allocations_update_status"),

    path('monthly_allocations/', views.monthly_allocations, name='monthly_allocations'),
    path('get_applicable_ioms/', views.get_applicable_ioms, name='get_applicable_ioms'),
    path('get_iom_details/', views.get_iom_details, name='get_iom_details'),
    path('allocations_ldap_search/', views.ldap_search, name='allocations_ldap_search'),
    # ensure save_monthly_allocations and save_team_allocation exist and are named accordingly in urls

    path('projects/team-allocations/save/', views.save_team_allocation, name='save_team_allocation'),

    path('get_allocations_for_iom/', views.get_allocations_for_iom, name='get_allocations_for_iom'),
    path('save_monthly_allocations/', views.save_monthly_allocations, name='save_monthly_allocations'),
    path("export_allocations/", views.export_allocations, name="export_allocations"),
    path('my-allocations/', views.my_allocations, name='my_allocations'),
    path('my-allocations/save-weekly/', views.save_my_alloc_weekly, name='save_my_alloc_weekly'),
    path('my-allocations/save-daily/', views.save_my_alloc_daily, name='save_my_alloc_daily'),
    path('my-allocations/export/excel/', views.export_my_punches_excel, name='export_my_punches_excel'),
    path('my-allocations/export/pdf/', views.export_my_punches_pdf, name='export_my_punches_pdf'),
    path("team-allocations/save-distribution/", views.save_team_distribution_using_team_table, name="save_team_distribution"),
    path("team-allocations/apply-distributions/", views.apply_team_distributions_view, name="apply_team_distributions"),
    path("team-allocations/delete-distribution/", views.delete_team_distribution, name="delete_team_distribution"),
]
