from django.contrib.auth import authenticate
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.serializers import TokenRefreshSerializer
from rest_framework_simplejwt.tokens import RefreshToken

from dashboard.utils import success_response


class LoginView(APIView):
    permission_classes = [AllowAny]
    authentication_classes = []

    def post(self, request):
        username = request.data.get("username")
        password = request.data.get("password")
        user = authenticate(username=username, password=password)
        if not user:
            return Response(
                {"success": False, "data": None, "errors": [{"field": "credentials", "message": "Invalid credentials"}]},
                status=status.HTTP_401_UNAUTHORIZED,
            )
        refresh = RefreshToken.for_user(user)
        payload = {
            "access": str(refresh.access_token),
            "refresh": str(refresh),
            "user": {
                "id": user.id,
                "username": user.username,
                "email": user.email or "",
                "isStaff": user.is_staff,
            },
        }
        return Response(success_response(payload), status=status.HTTP_200_OK)


class RefreshTokenView(APIView):
    permission_classes = [AllowAny]
    # Do not run JWTAuthentication here: clients send an expired access token in
    # Authorization while posting a valid refresh body; validating the header first
    # returns 401 before TokenRefreshSerializer runs.
    authentication_classes = []

    def post(self, request):
        serializer = TokenRefreshSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        return Response(success_response(serializer.validated_data), status=status.HTTP_200_OK)
