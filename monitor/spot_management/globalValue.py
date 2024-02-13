from datetime import datetime, timezone, timedelta


# searching time
TODAY = datetime.now(timezone.utc) + timedelta(hours=9)
ENDDAY = TODAY
STARTDAY = ENDDAY + timedelta(days=-1)


# UTC+0 time-line (STARTDAY ~ ENDDAY)
ENDTIME = int(datetime(int(ENDDAY.strftime("%Y")), int(ENDDAY.strftime("%m")), int(ENDDAY.strftime("%d")), 0, 0, 0).timestamp())
STARTTIME = int(datetime(int(STARTDAY.strftime("%Y")), int(STARTDAY.strftime("%m")), int(STARTDAY.strftime("%d")), 0, 0, 0).timestamp())


# Today Log
instancesLog = {}