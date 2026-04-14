from django.utils.dateparse import parse_date


def _first_scalar(val):
    if isinstance(val, (list, tuple)) and len(val) > 0:
        return val[0]
    return val


def parse_dashboard_date_range(request):
    """
    Same semantics as sunbright-dashboard `dateWhere` / `filterParams`:
    filter projects by customer_since (CX uses install_date — see analytics).
    Accepts date_from / date_to (REST) or dateFrom / dateTo (camelCase).
    Merges query string with JSON/form body so POST endpoints (e.g. insights) can pass the same range.
    """
    merged = {}
    qp = getattr(request, "query_params", None)
    if qp is not None:
        merged.update({k: _first_scalar(v) for k, v in qp.items()})
    elif hasattr(request, "GET"):
        merged.update({k: _first_scalar(v) for k, v in request.GET.items()})

    data = getattr(request, "data", None)
    if data is not None:
        for key in ("date_from", "date_to", "dateFrom", "dateTo"):
            if hasattr(data, "get"):
                val = data.get(key)
            elif isinstance(data, dict):
                val = data.get(key)
            else:
                val = None
            val = _first_scalar(val)
            if val not in (None, ""):
                merged[key] = val

    raw_from = merged.get("date_from") or merged.get("dateFrom")
    raw_to = merged.get("date_to") or merged.get("dateTo")
    date_from = parse_date(str(raw_from)) if raw_from else None
    date_to = parse_date(str(raw_to)) if raw_to else None
    return date_from, date_to


def success_response(data):
    return {"success": True, "data": data, "errors": []}


def error_response(errors):
    return {"success": False, "data": None, "errors": errors}
