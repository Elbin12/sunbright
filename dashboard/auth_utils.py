from typing import Optional

from django.contrib.auth.models import User

from dashboard.models import DashboardDataScope


def data_scope_payload(user: Optional[User]) -> Optional[dict]:
    """Summary for JWT + admin user list (camelCase for frontend)."""
    if user is None or not user.is_authenticated:
        return None
    if user.is_staff:
        return {
            "scopeKind": "organization",
            "label": "Organization (full access)",
            "restricted": False,
            "salesTeam": "",
            "salesRep": "",
            "salesTeams": [],
        }
    try:
        ds = user.dashboard_scope
    except DashboardDataScope.DoesNotExist:
        return {
            "scopeKind": "unset",
            "label": "No scope — assign team or rep to show data",
            "restricted": True,
            "salesTeam": "",
            "salesRep": "",
            "salesTeams": [],
        }

    if ds.scope_kind == DashboardDataScope.ScopeKind.TEAM:
        st = (ds.sales_team or "").strip()
        return {
            "scopeKind": "team",
            "label": f"Team: {st}" if st else "Team (incomplete)",
            "restricted": True,
            "salesTeam": ds.sales_team or "",
            "salesRep": "",
            "salesTeams": [],
        }
    if ds.scope_kind == DashboardDataScope.ScopeKind.TEAMS:
        names = [n.strip() for n in (ds.sales_teams or []) if isinstance(n, str) and n.strip()]
        preview = ", ".join(names[:4]) + ("…" if len(names) > 4 else "")
        return {
            "scopeKind": "teams",
            "label": f"Teams: {preview}" if preview else "Teams (incomplete)",
            "restricted": True,
            "salesTeam": "",
            "salesRep": "",
            "salesTeams": names,
        }
    rep = (ds.sales_rep or "").strip()
    return {
        "scopeKind": "rep",
        "label": f"Rep: {rep}" if rep else "Rep (incomplete)",
        "restricted": True,
        "salesTeam": "",
        "salesRep": ds.sales_rep or "",
        "salesTeams": [],
    }


def user_auth_payload(user: User) -> dict:
    """Serialize user for JWT login responses (matches sunbright-dashboard role model)."""
    role = "admin" if user.is_staff else "user"
    return {
        "id": user.id,
        "username": user.username,
        "email": user.email or "",
        "isStaff": user.is_staff,
        "role": role,
        "dataScope": data_scope_payload(user),
    }


def user_admin_list_row(user: User) -> dict:
    """Extended row for admin user-management UI."""
    row = user_auth_payload(user)
    row["isActive"] = user.is_active
    row["dateJoined"] = user.date_joined.isoformat() if user.date_joined else None
    return row
