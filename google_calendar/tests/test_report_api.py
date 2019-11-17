from datetime import timedelta, datetime
from unittest import mock
from unittest.mock import PropertyMock

import pandas as pd
from django.contrib.auth.models import User
from django.test import TestCase

from google_calendar.api import ReportCalculator
from google_calendar.models import UserMetaData


class TestReportCalculator(TestCase):

    def setUp(self):
        self.user = User.objects.create(username='test_user')
        self.user_meta_data = UserMetaData.objects.create(
            user=self.user,
            refresh_token='XXX',
            access_token='XXX',
            time_zone='Asia/Kolkata'
        )

    @mock.patch('google_calendar.api.ReportCalculator._monthly_events_df',
                new_callable=PropertyMock)
    @mock.patch('google_calendar.api.ReportCalculator._number_of_weeks',
                new_callable=PropertyMock)
    def test_number_of_events(self, mocked_week_count, mocked_monthly_df):
        """
        Tests calculated stats by mocking _monthly_events_df and _number_of_weeks
        data frame
        """
        mocked_week_count.return_value = 48
        df = pd.DataFrame({
            'year': [2019] * 5,
            'month': [1, 11, 2, 9, 7],
            'count': [5, 8, 10, 15, 18]
        })
        mocked_monthly_df.return_value = df
        calc = ReportCalculator(self.user)
        calc.last_3_month_date = datetime(2019, 9, 1)

        # Checking response
        result = calc.number_of_events()
        self.assertEqual(result['total'], 56)
        self.assertEqual(result['last_3_months'],[
            {'month': '2019-09', 'value': 15},
            {'month': '2019-11', 'value': 8},
        ])
        self.assertEqual(result['most'], [
            {'month': '2019-07', 'value': 18}
        ])
        self.assertEqual(result['least'], [
            {'month': '2019-01', 'value': 5}
        ])
        self.assertEqual(result['weekly_average'], round(56/48, 2))

        # Checking where mocked properties are called
        mocked_week_count.assert_called_once_with()
        mocked_monthly_df.assert_called_once_with()

        # Checking response when monthly data frame is empty
        df = pd.DataFrame(
            columns=['year', 'month', 'count']
        )
        df['count'] = df['count'].astype(int)
        mocked_monthly_df.return_value = df
        result_dict = calc.number_of_events()
        self.assertEqual(result_dict['total'], 0)
        self.assertEqual(result_dict['last_3_months'], [])
        self.assertEqual(result_dict['most'], [])
        self.assertEqual(result_dict['least'], [])
        self.assertEqual(result_dict['weekly_average'], 0)

    @mock.patch('google_calendar.api.ReportCalculator._monthly_events_df',
                new_callable=PropertyMock)
    @mock.patch('google_calendar.api.ReportCalculator._number_of_weeks',
                new_callable=PropertyMock)
    def test_time_spent(self, mocked_week_count, mocked_monthly_df):
        """
        Tests calculated stats by mocking _monthly_events_df and _number_of_weeks
        """
        mocked_week_count.return_value = 48
        df = pd.DataFrame({
            'year': [2019] * 5,
            'month': [1, 11, 2, 9, 7],
            'duration': [timedelta(hours=4), timedelta(hours=1),
                         timedelta(hours=32), timedelta(hours=2),
                         timedelta(hours=9)]
        })
        mocked_monthly_df.return_value = df
        calc = ReportCalculator(self.user)
        calc.last_3_month_date = datetime(2019, 9, 1)

        # Checking response
        result = calc.time_spent()
        self.assertEqual(result['total'], '2 days 00:00:00')
        self.assertEqual(result['last_3_months'], [
            {'month': '2019-09', 'value': '0 days 02:00:00'},
            {'month': '2019-11', 'value': '0 days 01:00:00'},
        ])
        self.assertEqual(result['most'], [
            {'month': '2019-02', 'value': '1 days 08:00:00'}
        ])
        self.assertEqual(result['least'], [
            {'month': '2019-11', 'value': '0 days 01:00:00'}
        ])

        self.assertEqual(result['weekly_average'], '0 days 01:00:00')

        # Checking where mocked properties are called
        mocked_week_count.assert_called_once_with()
        mocked_monthly_df.assert_called_once_with()

        # Checking response when monthly data frame is empty
        df = pd.DataFrame(
            columns=['year', 'month', 'duration']
        )
        df['duration'] = pd.to_timedelta(df['duration'])
        mocked_monthly_df.return_value = df
        result_dict = calc.time_spent()
        self.assertEqual(result_dict['total'], '0 days 00:00:00')
        self.assertEqual(result_dict['last_3_months'], [])
        self.assertEqual(result_dict['most'], [])
        self.assertEqual(result_dict['least'], [])
        self.assertEqual(result_dict['weekly_average'], '0 days 00:00:00')

    @mock.patch('google_calendar.api.ReportCalculator._attendees_counts',
                new_callable=PropertyMock)
    def test_times_pent(self, mocked_attendees_count):
        """
        Tests calculated stats by mocking _attendees_counts
        :return:
        """
        series = pd.Series(dict(
            zip([ f'{i}@gmail.com' for i in 'abcdef'],
                [11, 9, 7, 6, 5, 7])
        ))
        mocked_attendees_count.return_value = series

        # Checking response
        calc = ReportCalculator(self.user)
        result_dict = calc.attendee()
        self.assertEqual(result_dict['top_attendees'], [
            {'name': 'a@gmail.com', 'number_of_events': 11},
            {'name': 'b@gmail.com', 'number_of_events': 9},
            {'name': 'c@gmail.com', 'number_of_events': 7},
            {'name': 'f@gmail.com', 'number_of_events': 7}
        ])

        # Checking response for empty series
        mocked_attendees_count.return_value = pd.Series()
        calc = ReportCalculator(self.user)
        result_dict = calc.attendee()
        self.assertEqual(result_dict['top_attendees'], [])