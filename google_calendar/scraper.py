import pytz
from dateutil import parser
from django.conf import settings
from django.db import transaction
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from google_calendar import API_NAME, API_VERSION, ACCEPTED, SCOPES
from google_calendar.models import Event, Attendee


def create_event(record, user):
    """
    Creates Event and its related data with the data received from API
    :param record: dict received from API
    :param user: User instance
    :return: Event instance
    """
    # If the event is cancelled, we don't need to store it
    if record['status'] == 'cancelled':
        return
    time_zone = pytz.timezone(user.cal_meta_data.time_zone or 'UTC')
    event = Event()
    event.user = user
    event.event_id = record['id']
    event.summary = record['summary']
    event.description = record.get('description', '')
    event.location = record.get('location', '')
    event.is_creator = record['creator'].get('self', False)
    if not event.is_creator:
        event.creator_email = record['creator'].get('email', '')
    # Defaulting below field to False but it will be updated once we process
    # attendees list
    event.is_attendee = False

    start, end = record['start'], record['end']
    if start.get('dateTime'):
        event.start_datetime = parser.parse(start['dateTime'])
    else:
        event.start_datetime = time_zone.localize(parser.parse(start['date']))
    if end.get('dateTime'):
        event.end_datetime = parser.parse(end['dateTime'])
    else:
        event.end_datetime = time_zone.localize(parser.parse(end['date']))
    event.created_at = parser.parse(record['created'])
    event.save()
    create_attendees(event, record.get('attendees', []))
    return event


def create_attendees(event, attendees_dict):
    """
    Creates Attendee for an single Event
    :param event:
    :param attendees_dict:
    :return:
    """
    attendees_list = []
    for record in attendees_dict:
        attendee = Attendee()
        attendee.event = event
        attendee.email = record.get('email', '')
        # Converting camelCase to snake_case
        attendee.response = ''.join(
            i if i.islower() else f'_{i.lower()}' for i
            in record['responseStatus']
        )
        if record.get('self') and record.get('responseStatus') == ACCEPTED:
            event.is_attendee = True
        else:
            attendees_list.append(attendee)
    Attendee.objects.bulk_create(attendees_list)
    event.save()


def store_events(user):
    """
    It fetches events from Google calendar API and stores them in the
    respective models. It loads data in full replace. This will be only called
    only for the first time after user is registeres, after that

    *Note*: Authorization information of an user needs to be stored in UserMetaData
    model before calling this function
    :param user: User for which event will be stored
    """
    user_meta_data = user.cal_meta_data

    # Connecting to API
    creds = Credentials(
        token=user_meta_data.access_token,
        refresh_token=user_meta_data.refresh_token,
        token_uri=settings.GOOGLE_TOKEN_URI,
        client_id=settings.GOOGLE_CLIENT_ID,
        client_secret=settings.GOOGLE_CLIENT_SECRET,
        scopes=SCOPES
    )
    service = build(API_NAME, API_VERSION, credentials=creds)
    events_api = service.events()
    req = events_api.list(
        calendarId='primary',
        maxResults=2500,
        maxAttendees=1000
    )

    # Deleting existing events
    Attendee.objects.filter(event__user=user).delete()
    Event.objects.filter(user=user).delete()

    # Processing the API response and creating events
    while req:
        resp = req.execute()
        with transaction.atomic():
            for record in resp['items']:
                create_event(record, user)
        # Requesting next page
        req = events_api.list_next(req, resp)

    # Setting up time zone for the user

    req = service.settings().get(setting='timezone')
    user_meta_data.time_zone = req.execute()['value']
    user_meta_data.save()