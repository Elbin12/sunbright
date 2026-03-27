from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

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
from dashboard.services.sunbase_sync_service import get_last_sync_result, run_full_sync
from dashboard.utils import error_response, parse_dashboard_date_range, success_response


class CleanDealsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_clean_deals_bundle(d0, d1)))


class RetentionView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_retention_bundle(d0, d1)))


class PerformanceView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_performance_bundle(d0, d1)))


class PipelineView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_pipeline_bundle(d0, d1)))


class ProjectsOnHoldView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        serializer = ProjectSerializer(get_on_hold_projects(d0, d1), many=True)
        return Response(success_response(serializer.data))


class ProjectsCancelledView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        serializer = ProjectSerializer(get_cancelled_projects(d0, d1), many=True)
        return Response(success_response(serializer.data))


class CustomerExperienceView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_cx_bundle(d0, d1)))


class ManagerPerformanceView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_manager_bundle(d0, d1)))


class InsightsGenerateView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        return Response(
            success_response(
                {
                    "executiveSummary": "Connect your LLM provider and data warehouse to generate AI insights.",
                    "keyMetrics": [],
                    "repInsights": [],
                    "teamInsights": [],
                    "retentionInsights": [],
                    "cxInsights": [],
                    "actionItems": [],
                }
            )
        )


class DataSyncView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.is_staff:
            return Response(
                error_response([{"field": "auth", "message": "Only admins can trigger sync"}]),
                status=403,
            )
        result = run_full_sync()
        return Response(success_response(result))

    def get(self, request):
        return Response(success_response({"lastResult": get_last_sync_result()}))
