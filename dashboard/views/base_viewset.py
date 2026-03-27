from rest_framework import filters, viewsets


class BaseViewSet(viewsets.ModelViewSet):
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = []
    ordering_fields = "__all__"
    ordering = ["-id"]
