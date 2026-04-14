from rest_framework.permissions import BasePermission


class IsDashboardAdmin(BasePermission):
    """Full dashboard operations (manager analytics, AI insights, raw project API, sync)."""

    message = "Administrator role required for this resource."

    def has_permission(self, request, view):
        user = request.user
        return bool(user and user.is_authenticated and user.is_staff)


__all__ = ["IsDashboardAdmin"]
