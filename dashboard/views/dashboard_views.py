from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from dashboard.services.project_service import (
    get_cancellation_reasons_breakdown,
    get_category_breakdown,
    get_on_hold_reasons_breakdown,
    get_overview_metrics,
)
from dashboard.utils import parse_dashboard_date_range, success_response


class OverviewView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_overview_metrics(d0, d1)))


class CategoryBreakdownView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_category_breakdown(d0, d1)))


class CancellationReasonsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_cancellation_reasons_breakdown(d0, d1)))


class OnHoldReasonsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        d0, d1 = parse_dashboard_date_range(request)
        return Response(success_response(get_on_hold_reasons_breakdown(d0, d1)))
