from django.conf import settings
from django.contrib.auth import authenticate
from django.contrib.auth.models import User
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.serializers import TokenRefreshSerializer
from rest_framework_simplejwt.tokens import RefreshToken

from dashboard.auth_utils import user_auth_payload
from dashboard.utils import error_response, success_response


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
            "user": user_auth_payload(user),
        }
        return Response(success_response(payload), status=status.HTTP_200_OK)


class GoogleLoginView(APIView):
    """
    Accepts a Google ID token from the GIS client (body: { "credential": "<jwt>" }).
    Creates or updates a Django user and returns the same JWT pair as password login.
    """

    permission_classes = [AllowAny]
    authentication_classes = []

    def post(self, request):
        if not settings.GOOGLE_OAUTH_CLIENT_ID:
            return Response(
                error_response(
                    [{"field": "config", "message": "Google Sign-In is not configured on the server (GOOGLE_OAUTH_CLIENT_ID)"}]
                ),
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        token = request.data.get("credential") or request.data.get("id_token")
        if not token or not isinstance(token, str):
            return Response(
                error_response([{"field": "credential", "message": "Google credential token is required"}]),
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            idinfo = google_id_token.verify_oauth2_token(
                token,
                google_requests.Request(),
                settings.GOOGLE_OAUTH_CLIENT_ID,
            )
        except ValueError:
            return Response(
                error_response([{"field": "credential", "message": "Invalid Google token"}]),
                status=status.HTTP_401_UNAUTHORIZED,
            )

        if idinfo.get("iss") not in ("accounts.google.com", "https://accounts.google.com"):
            return Response(
                error_response([{"field": "credential", "message": "Invalid token issuer"}]),
                status=status.HTTP_401_UNAUTHORIZED,
            )

        email = (idinfo.get("email") or "").strip().lower()
        if not email:
            return Response(
                error_response([{"field": "email", "message": "Google account has no email"}]),
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not idinfo.get("email_verified"):
            return Response(
                error_response([{"field": "email", "message": "Verify your Google email before signing in"}]),
                status=status.HTTP_403_FORBIDDEN,
            )

        domain = email.split("@")[-1] if "@" in email else ""
        allowed = settings.GOOGLE_OAUTH_ALLOWED_DOMAIN
        if allowed and domain != allowed:
            return Response(
                error_response(
                    [
                        {
                            "field": "email",
                            "message": f"Sign-in is restricted to @{allowed} accounts",
                        }
                    ]
                ),
                status=status.HTTP_403_FORBIDDEN,
            )

        sub = idinfo.get("sub") or email
        username_base = email[:150] if len(email) <= 150 else f"g_{sub}"[:150]

        user = User.objects.filter(email__iexact=email).first()
        if user is None:
            user = User.objects.filter(username=username_base).first()

        is_admin_email = email in settings.GOOGLE_OAUTH_ADMIN_EMAILS

        if user is None:
            user = User(username=username_base, email=email, is_active=True)
            user.set_unusable_password()
            if is_admin_email:
                user.is_staff = True
            user.save()
        else:
            updates = []
            if not user.email or user.email.lower() != email:
                user.email = email
                updates.append("email")
            if is_admin_email and not user.is_staff:
                user.is_staff = True
                updates.append("is_staff")
            if updates:
                user.save(update_fields=updates)

        refresh = RefreshToken.for_user(user)
        payload = {
            "access": str(refresh.access_token),
            "refresh": str(refresh),
            "user": user_auth_payload(user),
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
