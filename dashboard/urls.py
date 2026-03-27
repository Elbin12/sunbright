from django.urls import path
from rest_framework.routers import DefaultRouter

from dashboard.views.analytics_views import (
    CleanDealsView,
    CustomerExperienceView,
    DataSyncView,
    InsightsGenerateView,
    ManagerPerformanceView,
    PerformanceView,
    PipelineView,
    ProjectsCancelledView,
    ProjectsOnHoldView,
    RetentionView,
)
from dashboard.views.auth_views import LoginView, RefreshTokenView
from dashboard.views.dashboard_views import (
    CancellationReasonsView,
    CategoryBreakdownView,
    OnHoldReasonsView,
    OverviewView,
)
from dashboard.views.project_viewset import ProjectViewSet

router = DefaultRouter()
router.register("projects", ProjectViewSet, basename="projects")

urlpatterns = [
    path("auth/login/", LoginView.as_view(), name="auth-login"),
    path("auth/refresh/", RefreshTokenView.as_view(), name="auth-refresh"),
    path("dashboard/overview/", OverviewView.as_view(), name="dashboard-overview"),
    path("dashboard/category-breakdown/", CategoryBreakdownView.as_view(), name="dashboard-category-breakdown"),
    path(
        "dashboard/cancellation-reasons/",
        CancellationReasonsView.as_view(),
        name="dashboard-cancellation-reasons",
    ),
    path("dashboard/on-hold-reasons/", OnHoldReasonsView.as_view(), name="dashboard-on-hold-reasons"),
    path("clean-deals/", CleanDealsView.as_view(), name="clean-deals"),
    path("retention/", RetentionView.as_view(), name="retention"),
    path("performance/", PerformanceView.as_view(), name="performance"),
    path("pipeline/", PipelineView.as_view(), name="pipeline"),
    path("projects/on-hold/", ProjectsOnHoldView.as_view(), name="projects-on-hold"),
    path("projects/cancelled/", ProjectsCancelledView.as_view(), name="projects-cancelled"),
    path("cx/", CustomerExperienceView.as_view(), name="cx"),
    path("manager/", ManagerPerformanceView.as_view(), name="manager"),
    path("insights/generate/", InsightsGenerateView.as_view(), name="insights-generate"),
    path("sync/", DataSyncView.as_view(), name="sync"),
]

urlpatterns += router.urls
