from datetime import datetime

import google_auth_oauthlib.flow
import pandas as pd
import pytz
from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.db.models import ExpressionWrapper, F, DurationField, Sum, Count
from django.shortcuts import redirect
from django.urls import reverse
from django.utils.functional import cached_property
from oauth2_provider.admin import AccessToken
from oauth2_provider.contrib.rest_framework import OAuth2Authentication
from rest_framework import permissions
from rest_framework.authentication import TokenAuthentication
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response
from rest_framework.views import APIView

from google_calendar import ACCEPTED, SCOPES, scraper
from google_calendar.models import Event, Attendee, UserMetaData


class AuthorizeAPI(APIView):
    permission_classes = (permissions.IsAuthenticated,)
    authentication_classes = (OAuth2Authentication,)

    def get(self, request):
        flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
            settings.GOOGLE_CRED_PATH,
            SCOPES)
        flow.redirect_uri = request.build_absolute_uri(
            reverse('google_calendar:oauth2_callback')
        )
        print(request.META)
        authorization_url, state = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true',
            state=request.META['HTTP_AUTHORIZATION'].split()[-1]
        )
        print(state)
        return redirect(authorization_url)


class OAuth2CallBackAPI(APIView):
    def get(self, request):
        state = request.GET['state']
        try:
            user = AccessToken.objects.get(token=state).user
        except AccessToken.DoesNotExist:
            raise PermissionDenied
        flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
            settings.GOOGLE_CRED_PATH, scopes=SCOPES, state=state)
        flow.redirect_uri = request.build_absolute_uri(
            reverse('google_calendar:oauth2_callback')
        )
        path = request.build_absolute_uri(
            request.get_full_path()
        )
        flow.fetch_token(authorization_response=path)
        credentials = flow.credentials
        UserMetaData.objects.create(user=user,
                                 access_token=credentials.token,
                                 refresh_token=credentials.refresh_token)
        scraper.store_events(user)
        return Response()


class ReportAPI(APIView):
    """
    Serves report containing several stats about metrics
    """

    permission_classes = (permissions.IsAuthenticated,)
    authentication_classes = (OAuth2Authentication, TokenAuthentication)
    def get(self, request):
        user = request.user
        extra_filters = {}
        search = request.GET.get('search')
        if search:
            extra_filters['summary__icontains'] = search
        calculator = ReportCalculator(user, **extra_filters)
        response = {}
        response['events'] = calculator.events()
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
                duration=ExpressionWrapper(
                    F('end_datetime') - F('start_datetime'),
                       output_field=DurationField()
                )
            ).values(
                'start_datetime__year',
                'start_datetime__month'

            ).annotate(
                duration=Sum('duration'),
                count=Count('*')
            )
        ), columns=[
            'start_datetime__year', 'start_datetime__month', 'duration', 'count'
        ])
        df.rename(columns={
            'start_datetime__year': 'year',
            'start_datetime__month': 'month',
        }, inplace=True)
        return df

    @cached_property
    def _weekly_events_df(self):
        """
        Creates a data frame containing events aggregated over week
        :return: DataFrame()
        """
        df = pd.DataFrame(list(
            self.event_queryset.annotate(
                duration=ExpressionWrapper(
                    F('end_datetime') - F('start_datetime'),
                    output_field=DurationField()
                )
            ).values(
                'start_datetime__year',
                'start_datetime__week'

            ).annotate(
                duration=Sum('duration'),
                count=Count('*')
            )
        ), columns=[
            'start_datetime__year', 'start_datetime__week', 'duration', 'count'
        ])
        df.rename(columns={
            'start_datetime__year': 'year',
            'start_datetime__month': 'week',
        }, inplace=True)
        return df

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
            ).order_by('-count').values_list(
                'email',
                'count'
            )
        ))

    def events(self):
        """
        Calculates dict for several stats for a `events` metrics
        - Total
        - Last 3 months distribution
        - Months with most number of events
        - Months with least number of events
        - Weekly average
        :return: dict
        """
        monthly_events_df = self._monthly_events_df
        weekly_events_df = self._weekly_events_df

        # Calculating months for most and least number of events
        sorted_by_count_df = monthly_events_df.sort_values(
            'count'
        )[['year', 'month', 'count']]

        sorted_by_count_df['count'] = sorted_by_count_df['count'].astype(int)
        if not sorted_by_count_df.empty:
            sorted_by_count_df['month'] = sorted_by_count_df.apply(
                lambda x: f"{x['year']}-{x['month']}", axis=1
            )
        del sorted_by_count_df['year']
        most_count = sorted_by_count_df.nlargest(1, 'count', keep='all')
        least_count = sorted_by_count_df.nsmallest(1, 'count', keep='all')

        # Calculating number of meetings for last 3 months
        last_3_months_df = monthly_events_df[
            (monthly_events_df['year'] >= self.last_3_month_date.year) &
            (monthly_events_df['month'] >= self.last_3_month_date.month)
            ][['month', 'year', 'count']]
        last_3_months_df.sort_values(['year', 'month'], inplace=True)
        if not last_3_months_df.empty:
            last_3_months_df['month'] = last_3_months_df.apply(
                lambda x: f"{x['year']}-{x['month']}", axis=1
            )
        del last_3_months_df['year']

        result = {}
        result['total'] = monthly_events_df['count'].sum()
        result['last_3_months'] = last_3_months_df.to_dict(orient='r')
        result['most_meetings'] = most_count.to_dict(orient='r')
        result['least_meetings'] = least_count.to_dict(orient='r')
        result['weekly_average'] = weekly_events_df['count'].mean().round(2)
        return result

    def time_spent(self):
        """
        Calculates dict for several stats for a `time_spent` metrics
        - Total
        - Last 3 months distribution
        - Months in which user spent most time
        - Months in which user spent least time
        - Weekly average
        :return: dict
        """
        monthly_events_df = self._monthly_events_df
        weekly_events_df = self._weekly_events_df

        # Calculating months for most and least amount of time spent
        sorted_by_count_df = monthly_events_df.sort_values(
            'duration'
        )[['year', 'month', 'duration']]

        sorted_by_count_df['duration'] = pd.to_timedelta(
            sorted_by_count_df['duration']
        )
        if not sorted_by_count_df.empty:
            sorted_by_count_df['month'] = sorted_by_count_df.apply(
                lambda x: f"{x['year']}-{x['month']}", axis=1
            )
        del sorted_by_count_df['year']
        most_time_spent = sorted_by_count_df.nlargest(1, 'duration', keep='all')
        least_time_spent = sorted_by_count_df.nsmallest(1, 'duration', keep='all')
        most_time_spent['duration'] = most_time_spent['duration'].map(str)
        least_time_spent['duration'] = least_time_spent['duration'].map(str)

        # Calculating time spent for past 3 months
        last_3_months_df = monthly_events_df[
            (monthly_events_df['year'] >= self.last_3_month_date.year) &
            (monthly_events_df['month'] >= self.last_3_month_date.month)
            ][['month', 'year', 'duration']]
        if not last_3_months_df.empty:
            last_3_months_df['month'] = last_3_months_df.apply(
                lambda x: f"{x['year']}-{x['month']}", axis=1
            )
        last_3_months_df['duration'] = last_3_months_df['duration'].map(str)
        del last_3_months_df['year']
        weekly_average = str(pd.Timedelta(
            seconds=weekly_events_df['duration'].dt.seconds.mean().round()
        ))

        result = {}
        result['total'] = str(monthly_events_df['duration'].sum())
        result['last_3_months'] = last_3_months_df.to_dict(orient='r')
        result['most_time_spent'] = most_time_spent.to_dict(orient='r')
        result['least_time_spent'] = least_time_spent.to_dict(orient='r')
        result['weekly_average'] = weekly_average
        return result

    def attendee(self):
        """
        Calculates dict containing several stats for attendee metrics
        - Top 3 people with whom user has attended the events most
        :return: dict
        """
        attendees_counts = self._attendees_counts
        return [
            {'name': key, 'count': val} for key, val
            in attendees_counts.nlargest(3, keep='all').items()
        ]
