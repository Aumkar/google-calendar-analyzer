from django.urls import path

from google_calendar.api import ReportApi

app_name = 'google_calendar'
urlpatterns = [
    path('report/', ReportApi.as_view())
]