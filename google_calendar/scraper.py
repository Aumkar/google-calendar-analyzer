from dateutil import parser
from django.conf import settings
from django.db import transaction
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from google_calendar import API_NAME, API_VERSION, ACCEPTED
from google_calendar.models import Event, Attendee


def create_event(record, user):
    """
    Create Event object with provided information
    :param record: dict received from API
    :param user: User instance
    :return: Event instance
    """
    event = Event()
    event.user = user
    event.event_id = record['id']
    event.summary = record['summary']
    event.description = record.get('description', '')
    event.location = record.get('location', '')
    event.is_creator = record['creator'].get('self', False)
    if not event.is_creator:
        event.creator_email = record['creator'].get('email', '')
    # Defaulting below field to False but it will be changed once user is found
    # in the attendee list
    event.is_attendee = False
    event.start_datetime = parser.parse(record['start']['dateTime'])
    event.end_datetime = parser.parse(record['end']['dateTime'])
    event.created_at = parser.parse(record['created'])
    event.save()
    create_attendees(event, record.get('attendees', []))
    return event


def create_attendees(event, attendees_dict):
    """
    Creates
    :param event:
    :param attendees_dict:
    :return:
    """
    attendees_list = []
    for record in attendees_dict:
        attendee = Attendee()
        attendee.event = event
        attendee.email = record.get('email', '')
        attendee.response = record['responseStatus']
        if record.get('self') and record.get('responseStatus') == ACCEPTED:
            event.is_attendee = True
        else:
            attendees_list.append(attendee)
    Attendee.objects.bulk_create(attendees_list)
    event.save()


def store_events(user):
    """
    It fetches events from Google calendar API and stores them in the
    respective models. It loads data in full replace.

    *Note*: Authorization information of an user needs to be stored in ______
    model before calling this function
    :param user: User for which event will be stored
    """
    user_meta_data = user.cal_meta_data
    creds = Credentials(
        token=user_meta_data.access_token,
        refresh_token=user_meta_data.refresh_token,
        token_uri=settings.GOOGLE_TOKEN_URI,
        client_id=settings.GOOGLE_CLIENT_ID,
        client_secret=settings.GOOGLE_CLIENT_SECRET
    )
    events_api = build(API_NAME, API_VERSION, credentials=creds).events()
    next_sync_token = None
    req = events_api.list(calendarId='primary',
                          syncToken=next_sync_token,
                          maxResults=2500)
    # Full replace
    Event.objects.filter(user=user).delete()

    # Adding records
    while req:
        resp = req.execute()
        next_sync_token = resp['nextSyncToken']
        # Process response
        with transaction.atomic():
            for record in resp['items']:
                create_event(record, user)
        req = events_api.list_next(req, resp)
