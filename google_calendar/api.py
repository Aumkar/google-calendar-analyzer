from datetime import datetime

import google_auth_oauthlib.flow
import numpy as np
import pandas as pd
import pytz
from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.db.models import ExpressionWrapper, F, DurationField, Sum, Count, \
    Min
from django.db.models.functions import Extract
from django.shortcuts import redirect
from django.urls import reverse
from django.utils.functional import cached_property
from oauth2_provider.contrib.rest_framework import OAuth2Authentication
from rest_framework import permissions
from rest_framework.authentication import SessionAuthentication
from rest_framework.response import Response
from rest_framework.views import APIView

from google_calendar import ACCEPTED, SCOPES, scraper
from google_calendar.models import Event, Attendee, UserMetaData


class AuthorizeAPI(APIView):
    """
    Authorizes user by redirecting them to google ouath API for
    completing user's consent
    """
    permission_classes = (permissions.IsAuthenticated,)
    authentication_classes = (SessionAuthentication,)

    def get(self, request):
        flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
            settings.GOOGLE_CRED_PATH,
            SCOPES)

        # After completion of user's consent google API will redirect request
        # to following url
        flow.redirect_uri = request.build_absolute_uri(
            reverse('google_calendar:oauth2_callback')
        )
        authorization_url, state = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true'
        )
        return redirect(authorization_url)


class OAuth2CallBackAPI(APIView):
    """
    Callback API which for google API will call after user's consent is completed
    """
    permission_classes = (permissions.IsAuthenticated,)
    authentication_classes = (SessionAuthentication,)

    def get(self, request):
        state = request.GET['state']
        flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
            settings.GOOGLE_CRED_PATH, scopes=SCOPES, state=state)
        flow.redirect_uri = request.build_absolute_uri(
            reverse('google_calendar:oauth2_callback')
        )
        path = request.get_full_path()
        flow.fetch_token(authorization_response=path)

        # Storing users credentials in the DB
        UserMetaData.objects.update_or_create(user=request.user, defaults={
            'access_token': flow.credentials.token,
            'refresh_token': flow.credentials.refresh_token
        })

        # Scraping Calendar events
        scraper.store_events(request.user)

        return Response()


class ReportAPI(APIView):
    """
    API to deliver report for calendar's events
    """
    permission_classes = (permissions.IsAuthenticated,)
    authentication_classes = (OAuth2Authentication, SessionAuthentication)

    def get(self, request):
        user = request.user

        extra_filters = {}
        search = request.GET.get('search')
        if search:
            extra_filters['summary__icontains'] = search

        calculator = ReportCalculator(user, **extra_filters)

        response = {}
        response['number_of_events'] = calculator.number_of_events()
        response['time_spent'] = calculator.time_spent()
        response['attendee'] = calculator.attendee()
        return Response(response)


class ReportCalculator(object):
    """
    Calculates several reports from Events and Attendee models based on below
    metrics
    - Number of events
    - Time spent in events
    - Attendee
    """
    def __init__(self, user, **kwargs):
        """
        :param user: User instance
        """
        self.user = user

        if user.cal_meta_data.time_zone:
            self.time_zone = pytz.timezone(user.cal_meta_data.time_zone)
        else:
            # Defaulting time_zone to UTC
            self.time_zone = pytz.utc

        self.last_3_month_date = datetime.today() - relativedelta(
            months=2, day=1, hour=0, minute=0, second=0, microsecond=0
        )
        attendee_filters = {
            'event__' + key: value for key,value in kwargs.items()
        }

        self.event_queryset = Event.objects.filter(
            user=user,
            is_attendee=True,
            start_datetime__lte=datetime.now(pytz.utc),
            **kwargs
        )
        self.attendee_queryset = Attendee.objects.filter(
            event__user=user,
            event__is_attendee=True,
            response=ACCEPTED,
            event__start_datetime__lte=datetime.now(pytz.utc),
            **attendee_filters
        )

    @cached_property
    def _monthly_events_df(self):
        """
        Creates a data frame containing events aggregated over months
        :return: DataFrame()
        """
        df = pd.DataFrame(list(
            self.event_queryset.annotate(
                time_spent=ExpressionWrapper(
                    F('end_datetime') - F('start_datetime'),
                    output_field=DurationField()
                )
            ).values(
                # Extracting year and month while localizing them
                year=Extract('start_datetime', 'year', tzinfo=self.time_zone),
                month=Extract('start_datetime', 'month', tzinfo=self.time_zone),
            ).annotate(
                time_spent=Sum('time_spent'),
                number_of_events=Count('*')
            ).order_by(
                'year',
                'month'
            )
        ), columns=[
            'year', 'month', 'time_spent', 'number_of_events'
        ])
        df['time_spent'] = pd.to_timedelta(df['time_spent'])
        df['number_of_events'] = df['number_of_events'].astype(int)
        return df

    @cached_property
    def _number_of_weeks(self):
        """
        Calculates number of week from first event till current date
        :return:
        """
        result_dict = self.event_queryset.aggregate(
            start=Min('start_datetime')
        )
        if result_dict['start']:
            # Days from first events
            total_days = (datetime.now(tz=self.time_zone) - result_dict['start']).days
            return round(total_days / 7)
        else:
            return 0

    @cached_property
    def _attendees_counts(self):
        """
        Creates a data frame containing count for each email of attendee
        who has attended event with the user
        :return:
        """
        return pd.Series(dict(
            self.attendee_queryset.values(
                'email'
            ).annotate(
                count=Count('*')
            ).values_list(
                'email',
                'count'
            )
        ))

    def number_of_events(self):
        """
        Calculates dict for several stats for a `number of events` metrics
        - Total
        - Last 3 months distribution(It may have less than 3 if user have not
        attended any event in whole month)
        - Months with most number of events
        - Months with least number of events
        - Weekly average
        :return: dict
        """
        # Calculating months for most and least number of events
        least_count, most_count = self._calculate_months_with_min_max(
            'number_of_events'
        )

        # Calculating number of events for last 3 months
        last_3_months_df = self._calculate_last_3_months('number_of_events')

        # Calculating weekly average
        week_count = self._number_of_weeks
        if week_count:
            weekly_average = np.round(
                self._monthly_events_df['number_of_events'].sum()/week_count, 2
            )
            weekly_average = weekly_average if not np.isnan(
                weekly_average) else 0
        else:
            weekly_average = 0

        # Structuring final report dict
        result = {}
        result['total'] = self._monthly_events_df['number_of_events'].sum()
        result['last_3_months'] = last_3_months_df.to_dict(orient='r')
        result['most'] = most_count.to_dict(orient='r')
        result['least'] = least_count.to_dict(orient='r')
        result['weekly_average'] = weekly_average
        return result

    def time_spent(self):
        """
        Calculates dict for several stats for a `time_spent` metrics
        - Total time spent
        - Last 3 months distribution
        - Months in which user spent most time
        - Months in which user spent least time
        - Weekly average
        :return: dict
        """
        # Calculating months for most and least amount of time spent
        least_time_spent, most_time_spent = self._calculate_months_with_min_max(
            'time_spent'
        )

        # Calculating time spent for past 3 months
        last_3_months_df = self._calculate_last_3_months('time_spent')

        # Casting pd.Timedelta to readable strings
        most_time_spent['value'] = most_time_spent['value'].map(str)
        least_time_spent['value'] = least_time_spent['value'].map(str)
        last_3_months_df['value'] = last_3_months_df['value'].map(str)

        # Calculating weekly average
        week_count = self._number_of_weeks
        if week_count:
            weekly_average = np.round(
                self._monthly_events_df['time_spent'].dt.total_seconds().sum() / week_count
            )
            weekly_average = str(pd.Timedelta(
                seconds=weekly_average if not np.isnan(weekly_average) else 0
            ))
        else:
            weekly_average = 0

        # Structuring final report dict
        result = {}
        result['total'] = str(self._monthly_events_df['time_spent'].sum())
        result['last_3_months'] = last_3_months_df.to_dict(orient='r')
        result['most'] = most_time_spent.to_dict(orient='r')
        result['least'] = least_time_spent.to_dict(orient='r')
        result['weekly_average'] = weekly_average
        return result

    def _calculate_months_with_min_max(self, metrics):
        """
        Calculates months with most and least count for a given metrics
        :param metrics: time_spent|number_of_events
        :return: DataFrame() with least count, DataFrame() for most count
        """
        df = self._monthly_events_df.sort_values(
            metrics
        )[['year', 'month', metrics]]
        df.rename(columns={metrics: 'value'}, inplace=True)

        # Combining year and month
        # i.e. 2018 + 1 --> `2018-01`
        if not df.empty:
            df['month'] = df.apply(
                lambda x: f"{x['year']}-{x['month']:02}", axis=1
            )
        del df['year']

        # Retrieving min and max records for a metrics. In case of tie,
        # it will pull all qualifying records
        most_time_spent = df.nlargest(1, 'value', keep='all')
        least_time_spent = df.nsmallest(1, 'value', keep='all')

        return least_time_spent, most_time_spent

    def _calculate_last_3_months(self, metrics):
        """
        Calculates data frame with last 3 months for given metrics
        :param metrics: time_spent|number_of_events
        :return: DataFrame
        """
        df = self._monthly_events_df[
            ['year', 'month', metrics]
        ].copy()
        df.rename(columns={metrics: 'value'}, inplace=True)

        # Retrieving records for past 3 months
        df = df[(df['year'] >= self.last_3_month_date.year) &
                (df['month'] >= self.last_3_month_date.month)][
            ['month', 'year', 'value']
        ]
        if not df.empty:
            # Combining year and month
            # i.e. 2018 + 1 --> `2018-01`
            df['month'] = df.apply(
                lambda x: f"{x['year']}-{x['month']:02}", axis=1
            )
        del df['year']
        return df

    def attendee(self):
        """
        Calculates dict containing several stats for attendee metrics
        - Top 3 people with whom user has attended the events most. But if there
        is a tie, then it may have more than 3 people
        :return: dict
        """
        attendees_counts = self._attendees_counts
        result = {}
        result['top_attendees'] = [
            {'name': key, 'number_of_events': val} for key, val
            in attendees_counts.nlargest(3, keep='all').items()
        ]
        return result
