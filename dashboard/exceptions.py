from rest_framework.views import exception_handler


def custom_exception_handler(exc, context):
    response = exception_handler(exc, context)
    if response is None:
        return response

    errors = []
    if isinstance(response.data, dict):
        for field, detail in response.data.items():
            if isinstance(detail, list):
                for item in detail:
                    errors.append({"field": field, "message": str(item)})
            else:
                errors.append({"field": field, "message": str(detail)})
    else:
        errors.append({"message": str(response.data)})

    response.data = {"success": False, "data": None, "errors": errors}
    return response
