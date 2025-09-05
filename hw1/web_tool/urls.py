from django.urls import path
from .views import (
    mme_form_page, mme_form,
    View_Perfect_Match_Table,
    View_by_Reference, View_by_Reference_data,
    View_by_Eptiope, View_by_Epitope_data,
    View_by_Query, View_by_Query_data,
    iedb_from_sqlite, api_create_job,
    job_id_search,view_by_ref_detail
)

urlpatterns = [
    path("", mme_form_page, name="mme_form_page"),
    path("mme_form/", mme_form, name="mme_form"),
    
    path("job_id_search/", job_id_search, name="job_id_search"),

    path("View_Perfect_Match_Table/", View_Perfect_Match_Table, name="View_Perfect_Match_Table"),

    path("View_by_Reference/", View_by_Reference, name="View_by_Reference"),
    path("View_by_Reference/data/", View_by_Reference_data, name="View_by_Reference_data"),

    path("View_by_Eptiope/", View_by_Eptiope, name="View_by_Eptiope"),
    path("api/view_by_epitope_data", View_by_Epitope_data, name="view_by_epitope_data"),

    path("View_by_Query/", View_by_Query, name="View_by_Query"),
    path("View_by_Query/data/", View_by_Query_data, name="View_by_Query_data"),

    path("api/iedb_from_sqlite/", iedb_from_sqlite, name="iedb_from_sqlite"),

    path("api/jobs/create/", api_create_job, name="api_create_job"),

    path("View_by_Reference/detail/", view_by_ref_detail, name="view_by_ref_detail"),
]