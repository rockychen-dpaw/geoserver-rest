import pytz
import os

from datetime import datetime,timezone

UTC = timezone.utc

TIMEZONE = pytz.timezone(os.environ.get("TZ","Australia/Perth"))

def get_timezone(tz):
    return pytz.timezone(tz) if tz else get_current_timezone()

def get_current_timezone():
    return TIMEZONE

def is_aware(dt):
     return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None

def is_naive(dt):
    return dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None

def localtime(dt=None,timezone=None):
    if timezone is None:
        timezone = get_current_timezone()
    
    # If `dt` is naive, astimezone() will raise a ValueError,
    # so we don't need to perform a redundant check.
    if not dt:
        return datetime.now(tz=timezone)
    elif dt.tzinfo == timezone:
        return dt

    dt = dt.astimezone(timezone)
    if hasattr(timezone, 'normalize'):
        # This method is available for pytz time zones.
        dt = timezone.normalize(dt)
    return dt

def parse(dt,pattern="%Y-%m-%d %H:%M:%S",timezone=None):
    return make_aware(datetime.strptime(dt,pattern),timezone=timezone)

def dbtime(dt=None):
    return localtime(dt).strftime("%Y-%m-%d %H:%M:%S%z")

def format(dt=None,pattern="%Y-%m-%d %H:%M:%S",timezone=None):
    return localtime(dt,timezone=timezone).strftime(pattern)

def format_timedelta(td,ignore_milliseconds = True):
    if not td:
        return "00:00:00.000"
    days = td.days
    seconds = td.seconds
    hours = int(seconds / 3600)
    seconds = seconds % 3600
    minutes = int(seconds / 60)
    seconds = seconds % 60


    """
    result = ("1 Day" if days == 1 else "{} Days".format(days))  if days > 0 else None
    result = "{}{}".format("{} ".format(result) if result else "",("1 Hour" if hours == 1 else "{} Hours".format(hours)))  if hours > 0 else result
    result = "{}{}".format("{} ".format(result) if result else "",("1 Minute" if minutes == 1 else "{} Minutes".format(minutes)))  if minutes > 0 else result
    result = "{}{}".format("{} ".format(result) if result else "",("1 Second" if seconds == 1 else "{} Seconds".format(seconds)))  if seconds > 0 else result
    
    if not ignore_milliseconds:
        milliseconds = int(td.microseconds / 1000)
        result = "{}{}".format("{} ".format(result) if result else "",("1 Millisecond" if milliseconds == 1 else "{} Milliseconds".format(milliseconds)))  if milliseconds > 0 else result
    """
    if ignore_milliseconds:
        return "{0:0=2d}:{1:0=2d}:{2:0=2d}".format(hours,minutes,seconds)
    else:
        milliseconds = int(td.microseconds / 1000)
        return "{0:0=2d}:{1:0=2d}:{2:0=2d}.{3:0=3d}".format(hours,minutes,seconds,milliseconds)

def timestamp(dt=None):
    return  localtime(dt,timezone=timezone.utc).timestamp()

def make_aware(dt, timezone=None):
    if timezone is None:
        timezone = get_current_timezone()
    if isinstance(timezone,str):
        timezone = pytz.timezone(timezone)

    if hasattr(timezone, 'localize'):
        # This method is available for pytz time zones.
        return timezone.localize(dt, is_dst=None)
    else:
        # Check that we won't overwrite the timezone of an aware datetime.
        if is_aware(dt):
            raise ValueError(
                "make_aware expects a naive datetime, got %s" % dt)
        # This may be wrong around DST changes!
        return dt.replace(tzinfo=timezone)




