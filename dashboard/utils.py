from django.utils.dateparse import parse_date


def parse_dashboard_date_range(request):
    """
    Same semantics as sunbright-dashboard `dateWhere` / `filterParams`:
    filter projects by customer_since (CX uses install_date — see analytics).
    Accepts date_from / date_to (REST) or dateFrom / dateTo (camelCase).
    """
    q = getattr(request, "query_params", getattr(request, "GET", {}))
    raw_from = q.get("date_from") or q.get("dateFrom")
    raw_to = q.get("date_to") or q.get("dateTo")
    date_from = parse_date(raw_from) if raw_from else None
    date_to = parse_date(raw_to) if raw_to else None
    return date_from, date_to


def success_response(data):
    return {"success": True, "data": data, "errors": []}


def error_response(errors):
    return {"success": False, "data": None, "errors": errors}
