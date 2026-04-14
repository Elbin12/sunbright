from django.contrib.auth.models import User
from django.core.exceptions import ValidationError as DjangoValidationError
from django.db.models import Q
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from dashboard.auth_utils import user_admin_list_row
from dashboard.models import DashboardDataScope
from dashboard.pagination import DefaultPagination
from dashboard.permissions import IsDashboardAdmin
from dashboard.utils import error_response, success_response


def _apply_dashboard_scope(user: User, raw) -> None:
    if not isinstance(raw, dict):
        raise ValueError("dataScope must be an object")
    kind_key = (raw.get("scopeKind") or raw.get("scope_kind") or "").strip().lower()
    mapping = {
        "team": DashboardDataScope.ScopeKind.TEAM,
        "teams": DashboardDataScope.ScopeKind.TEAMS,
        "rep": DashboardDataScope.ScopeKind.REP,
    }
    if kind_key not in mapping:
        raise ValueError('scopeKind must be "team", "teams", or "rep"')
    ds, _ = DashboardDataScope.objects.get_or_create(user=user)
    ds.scope_kind = mapping[kind_key]
    ds.sales_team = str(raw.get("salesTeam") or raw.get("sales_team") or "")[:255]
    ds.sales_rep = str(raw.get("salesRep") or raw.get("sales_rep") or "")[:255]
    teams_raw = raw.get("salesTeams") if "salesTeams" in raw else raw.get("sales_teams")
    if isinstance(teams_raw, list):
        ds.sales_teams = [str(x).strip() for x in teams_raw if str(x).strip()][:80]
    else:
        ds.sales_teams = []
    ds.full_clean()
    ds.save()


class DashboardUserListView(APIView):
    permission_classes = [IsAuthenticated, IsDashboardAdmin]

    def get(self, request):
        qs = User.objects.all().order_by("-date_joined")
        search = (request.query_params.get("search") or "").strip()
        if search:
            qs = qs.filter(Q(username__icontains=search) | Q(email__icontains=search))

        paginator = DefaultPagination()
        page = paginator.paginate_queryset(qs, request)
        if page is not None:
            items = [user_admin_list_row(u) for u in page]
            return Response(
                success_response(
                    {
                        "items": items,
                        "total": paginator.page.paginator.count,
                        "page": paginator.page.number,
                        "pageSize": paginator.page_size,
                    }
                )
            )
        items = [user_admin_list_row(u) for u in qs]
        return Response(success_response({"items": items, "total": len(items), "page": 1, "pageSize": len(items)}))


class DashboardUserDetailView(APIView):
    permission_classes = [IsAuthenticated, IsDashboardAdmin]

    def patch(self, request, pk):
        try:
            pk_int = int(pk)
        except (TypeError, ValueError):
            return Response(
                error_response([{"field": "id", "message": "Invalid user id"}]),
                status=400,
            )

        try:
            target = User.objects.get(pk=pk_int)
        except User.DoesNotExist:
            return Response(
                error_response([{"field": "id", "message": "User not found"}]),
                status=404,
            )

        body = request.data if isinstance(request.data, dict) else {}

        updates = []
        is_staff_provided = "isStaff" in body
        role_provided = "role" in body

        if is_staff_provided or role_provided:
            if is_staff_provided:
                next_staff = bool(body.get("isStaff"))
            else:
                next_staff = body.get("role") == "admin"

            staff_admins = User.objects.filter(is_staff=True).count()
            if target.is_staff and not next_staff and staff_admins <= 1:
                return Response(
                    error_response(
                        [
                            {
                                "field": "role",
                                "message": "Cannot remove the last administrator. Promote another user first.",
                            }
                        ]
                    ),
                    status=400,
                )

            target.is_staff = next_staff
            updates.append("is_staff")

        if "isActive" in body:
            next_active = bool(body.get("isActive"))
            if target.pk == request.user.pk and not next_active:
                return Response(
                    error_response(
                        [{"field": "isActive", "message": "You cannot deactivate your own account."}]
                    ),
                    status=400,
                )
            target.is_active = next_active
            updates.append("is_active")

        if updates:
            target.save(update_fields=updates)

        if body.get("clearDataScope") is True:
            if target.is_staff:
                return Response(
                    error_response(
                        [{"field": "dataScope", "message": "Administrators do not use data scope profiles."}]
                    ),
                    status=400,
                )
            DashboardDataScope.objects.filter(user=target).delete()
        elif "dataScope" in body:
            if target.is_staff:
                return Response(
                    error_response(
                        [{"field": "dataScope", "message": "Remove administrator role before assigning a data scope."}]
                    ),
                    status=400,
                )
            try:
                _apply_dashboard_scope(target, body.get("dataScope") or {})
            except ValueError as exc:
                return Response(
                    error_response([{"field": "dataScope", "message": str(exc)}]),
                    status=400,
                )
            except DjangoValidationError as exc:
                msgs = []
                if hasattr(exc, "error_dict"):
                    for field, errs in exc.error_dict.items():
                        for e in errs:
                            msgs.append({"field": field, "message": str(e)})
                elif getattr(exc, "messages", None):
                    for m in exc.messages:
                        msgs.append({"field": "dataScope", "message": str(m)})
                else:
                    msgs.append({"field": "dataScope", "message": str(exc)})
                return Response(error_response(msgs or [{"field": "dataScope", "message": "Invalid scope"}]), status=400)

        return Response(success_response(user_admin_list_row(target)))
