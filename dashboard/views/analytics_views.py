from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from dashboard.permissions import IsDashboardAdmin
from dashboard.serializers.project_serializer import ProjectSerializer
from dashboard.services.analytics_service import (
    get_clean_deals_bundle,
    get_cx_bundle,
    get_manager_bundle,
    get_performance_bundle,
    get_pipeline_bundle,
    get_retention_bundle,
)
from dashboard.services.project_service import get_cancelled_projects, get_on_hold_projects
from dashboard.services.insights_service import InsightsLLMError, generate_dashboard_insights
from dashboard.services.sunbase_sync_service import get_last_sync_result, run_full_sync
from dashboard.utils import error_response, parse_dashboard_date_range, success_response


class CleanDealsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_clean_deals_bundle(d0, d1, request.user)))


class RetentionView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_retention_bundle(d0, d1, request.user)))


class PerformanceView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_performance_bundle(d0, d1, request.user)))


class PipelineView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_pipeline_bundle(d0, d1, request.user)))


class ProjectsOnHoldView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        serializer = ProjectSerializer(get_on_hold_projects(d0, d1, request.user), many=True)
        return Response(success_response(serializer.data))


class ProjectsCancelledView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        serializer = ProjectSerializer(get_cancelled_projects(d0, d1, request.user), many=True)
        return Response(success_response(serializer.data))


class CustomerExperienceView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_cx_bundle(d0, d1, request.user)))


class ManagerPerformanceView(APIView):
    """D2D / appointment metrics; scoped by user data scope (non-admins see their team/rep slice)."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_manager_bundle(d0, d1, request.user)))


class InsightsGenerateView(APIView):
    permission_classes = [IsAuthenticated, IsDashboardAdmin]

    def post(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        try:
            payload = generate_dashboard_insights(d0, d1, request.user)
        except InsightsLLMError as exc:
            return Response(
                error_response([{"field": "llm", "message": str(exc)}]),
                status=502,
            )
        return Response(success_response(payload))


class DataSyncView(APIView):
    permission_classes = [IsAuthenticated, IsDashboardAdmin]

    def post(self, request):
        result = run_full_sync()
        return Response(success_response(result))

    def get(self, request):
        return Response(success_response({"lastResult": get_last_sync_result()}))
