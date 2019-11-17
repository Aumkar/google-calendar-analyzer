from datetime import datetime

import pytz
from django.contrib.auth.models import User
from django.test import TestCase

from google_calendar import scraper
from google_calendar.models import UserMetaData, Event


class TestScraper(TestCase):
    def setUp(self):
        self.user = User.objects.create(username='test_user',
                                        email='test_user@gmail.com')
        self.user_meta_data = UserMetaData.objects.create(
            user=self.user,
            refresh_token='XXX',
            access_token='XXX',
            time_zone='Asia/Kolkata'
        )

    def test_store_event(self):
        """
        Tests creation of Events along with respective attendees
        :return:
        """
        event_record = {
            'id': '3ia2h10m49f5gsfj2vqt278j7h_20191115T113000Z',
            'status': 'confirmed',
            'created': '2019-10-14T14:58:32.000Z',
            'summary': 'Test calendar event',
            'description': 'The purpose of this event is it test store_event()',
            'location': 'Pune',
            'creator': {'email': 'test_user@gmail.com', 'self': True},
            'start': {'dateTime': '2019-11-15T17:00:00+05:30'},
            'end': {'dateTime': '2019-11-15T17:30:00+05:30'},
            'attendees': [{'email': 'a@gmail.com',
                           'responseStatus': 'declined'},
                          {'email': 'b@gmail.com',
                           'responseStatus': 'accepted'},
                          {'email': 'test_user@gmail.com',
                           'self': True,
                           'responseStatus': 'accepted'},
                          {'email': 'c@kisanhub.com',
                           'responseStatus': 'needsAction'}]
        }
        scraper.create_event(event_record, self.user)
        created_event = Event.objects.first()

        # Checking event_record against entry created in the DB
        self.assertEqual(created_event.user_id, self.user.pk)
        self.assertEqual(created_event.event_id, event_record['id'])
        self.assertEqual(created_event.summary, event_record['summary'])
        self.assertEqual(created_event.description, event_record['description'])
        self.assertEqual(created_event.location, event_record['location'])
        self.assertEqual(created_event.is_creator, True)
        self.assertEqual(created_event.creator_email, '')
        self.assertEqual(created_event.is_attendee, True)
        self.assertEqual(created_event.created_at,
                         datetime(2019, 10, 14, 14, 58, 32, tzinfo=pytz.utc))
        self.assertEqual(created_event.start_datetime,
                         datetime(2019, 11, 15, 11, 30, tzinfo=pytz.utc))
        self.assertEqual(created_event.end_datetime,
                         datetime(2019, 11, 15, 12, tzinfo=pytz.utc))

        attendee_qset = created_event.attendees.all()
        expected_attendee_dict = {
            'a@gmail.com': 'declined',
            'b@gmail.com': 'accepted',
            'c@kisanhub.com': 'needs_action'
        }
        self.assertEqual(set(attendee_qset.values_list('email', flat=True)),
                         set(expected_attendee_dict.keys()))
        for attendee in attendee_qset:
            self.assertEqual(attendee.response,
                             expected_attendee_dict[attendee.email])

        created_event.delete()

        # When user is not creator or attendee
        event_record['creator'] = {'email': 'a@gmail.com'}
        event_record['attendees'][2]['responseStatus'] = 'declined'

        scraper.create_event(event_record, self.user)
        created_event = Event.objects.first()

        self.assertEqual(created_event.is_creator, False)
        self.assertEqual(created_event.is_attendee, False)
        self.assertEqual(created_event.creator_email,
                         event_record['creator']['email'])

