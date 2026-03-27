from rest_framework.permissions import IsAuthenticated

from dashboard.models import Project
from dashboard.serializers.project_serializer import ProjectSerializer
from dashboard.views.base_viewset import BaseViewSet


class ProjectViewSet(BaseViewSet):
    serializer_class = ProjectSerializer
    permission_classes = [IsAuthenticated]
    search_fields = ["first_name", "last_name", "sales_rep", "sales_team", "job_status"]

    def get_queryset(self):
        return Project.objects.filter(deleted_at__isnull=True)
