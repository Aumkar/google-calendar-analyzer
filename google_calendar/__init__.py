
# Google calendar API
API_NAME = 'calendar'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']

# Choices for attendee responses
NEEDS_ACTION = 'needs_action'
DECLINED = 'declined'
TENTATIVE = 'tentative'
ACCEPTED = 'accepted'

ATTENDEE_RESPONSES = (
    (NEEDS_ACTION, 'Needs Action'),
    (DECLINED, 'Declined'),
    (TENTATIVE, 'Tentative'),
    (ACCEPTED, 'Accepted')
)

