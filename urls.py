from django.contrib import admin
from django.urls import path, include

# API urlpatterns
api_urlpatterns = [
    path('google_calendar/', include('google_calendar.urls',
                                     namespace='google_calendar'))
]

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include(api_urlpatterns))
]
