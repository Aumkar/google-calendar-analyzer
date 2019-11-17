from django.urls import path

from google_calendar.api import ReportAPI, AuthorizeAPI, OAuth2CallBackAPI

app_name = 'google_calendar'
urlpatterns = [
    path('report/', ReportAPI.as_view(), name='report'),
    path('authorize/', AuthorizeAPI.as_view(), name='authorize'),
    path('oauth2_callback/', OAuth2CallBackAPI.as_view(), name='oauth2_callback'),
]