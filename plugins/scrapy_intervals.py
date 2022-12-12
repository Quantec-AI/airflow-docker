from datetime import timedelta
from pendulum import UTC, Date, DateTime, Time, timezone
from typing import Optional
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

UTC = timezone('UTC')

class USEquityScrapyTimetable(Timetable):
    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        weekday = run_after.weekday()
        delta = timedelta(days=1)
        if weekday in (0, 6):  # Monday and Sunday -- interval is last Friday.
            days_since_friday = (run_after.weekday() - 4) % 7
            delta = timedelta(days=days_since_friday)
        else:  # Otherwise the interval is yesterday.
            delta = timedelta(days=1)

        # 1) Run After 14:05 - 14:35 (05:05 - 05:35)
        # Data Interval: Previous 15:35 - Current 14:05 (06:35 - 05:05)
        if run_after >= run_after.set(hour=5, minute=5) and run_after <= run_after.set(hour=5, minute=35):
            start = (run_after-delta).set(hour=6, minute=35, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=5, minute=5, second=0).replace(tzinfo=UTC)

        # 2) Run After 14:35 - 15:05 (05:35 - 06:05)
        # Data Interval: 14:05 - 14:35 (05:05 - 05:35)
        elif run_after >= run_after.set(hour=5, minute=35) and run_after <= run_after.set(hour=6, minute=5):
            start = run_after.set(hour=5, minute=5, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=5, minute=35, second=0).replace(tzinfo=UTC)

        # 3) Run After 15:05 - 15:35 (06:05 - 06:35)
        # Data Interval: 14:35 - 15:05 (05:35 - 06:05)
        elif run_after >= run_after.set(hour=6, minute=5) and run_after <= run_after.set(hour=6, minute=35):
            start = run_after.set(hour=5, minute=35, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=6, minute=5, second=0).replace(tzinfo=UTC)

        # 4) Run After 15:35 - 16:05 (06:35 - 07:05)
        # Data Interval: 15:05 - 15:35 (06:05 - 06:35)
        elif run_after >= run_after.set(hour=6, minute=35) and run_after.hour <= 23:
            start = run_after.set(hour=6, minute=5, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=6, minute=35, second=0).replace(tzinfo=UTC)

        # 5) Run After 16:05 - 09:00 (07:05 - 00:00)
        # Data Interval: 15:35 - 16:05 (06:05 - 07:05)

        # 5) Run After Midnight - 14:05 (Midnight - 05:05)
        # Data Interval: Previous 15:05 - 15:35 (06:05 - 06:35)
        else:
            start = (run_after-delta).set(hour=6, minute=5, second=0).replace(tzinfo=UTC)
            end = (run_after-delta).set(hour=6, minute=35, second=0).replace(tzinfo=UTC)

        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is not None:  # There was a previous run on the regular schedule.
            last_start = last_automated_data_interval.start
            last_start_weekday = last_start.weekday()
            
            days_since_friday = (last_start_weekday - 4) % 7
            if 4 <= last_start_weekday <= 6:
                delta_since = timedelta(days=days_since_friday)
                delta = timedelta(days=3)
            else:
                delta_since = timedelta(days=0)
                delta = timedelta(days=1)

            # 1) 15:05 - 15:35 (06:05 - 06:35)
            # -> 15:35 - 14:05 (06:35 - 05:05)
            if last_start == last_start.set(hour=6, minute=5):
                next_start = (last_start-delta_since).set(hour=6, minute=35).replace(tzinfo=UTC)
                next_end = (last_start+delta).set(hour=5, minute=5).replace(tzinfo=UTC)

            # 2) 15:35 - 14:05 (06:35 - 05:05)
            # -> 14:05 - 14:35 (05:05 - 05:35)
            elif last_start == last_start.set(hour=6, minute=35):
                next_start = (last_start+delta).set(hour=5, minute=5).replace(tzinfo=UTC)
                next_end = (last_start+delta).set(hour=5, minute=35).replace(tzinfo=UTC)

            # 3) 14:05 - 14:35 (05:05 - 05:35)
            # -> 14:35 - 15:05 (05:35 - 06:05)
            elif last_start == last_start.set(hour=5, minute=5):
                next_start = last_start.set(hour=5, minute=35).replace(tzinfo=UTC)
                next_end = last_start.set(hour=6, minute=5).replace(tzinfo=UTC)

            # 4) 14:35 - 15:05 (05:35 - 06:05)
            # -> 15:05 - 15:35 (06:05 - 06:35)
            else:
                next_start = last_start.set(hour=6, minute=5).replace(tzinfo=UTC)
                next_end = last_start.set(hour=6, minute=35).replace(tzinfo=UTC)

        else:  # This is the first ever run on the regular schedule. First data interval will always start at 6:00 and end at 16:30
            next_start = restriction.earliest
            if next_start is None:  # No start_date. Don't schedule.
                return None
            if not restriction.catchup: # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(next_start, DateTime.combine(Date.today(), Time.min).replace(tzinfo=UTC))

#             elif next_start.time() != Time.min:
#                 # If earliest does not fall on midnight, skip to the next day.
#                 next_day = next_start.date() + timedelta(days=1)
#                 next_start = DateTime.combine(next_day, Time.min).replace(tzinfo=UTC)
            next_start_weekday = next_start.weekday()
            days_since_friday = (next_start_weekday - 4) % 7

            if next_start_weekday in (0,5,6):   # Saturday, Sunday, Monday - Friday to Monday
                delta = timedelta(days=days_since_friday)
                next_start = (next_start-delta)
                next_end = (next_start+timedelta(days=3))
            else:
                delta = timedelta(days=1)
                next_start = (next_start-delta)
                next_end = next_start

            next_start = next_start.set(hour=6, minute=35).replace(tzinfo=UTC)
            next_end = next_end.set(hour=5, minute=5).replace(tzinfo=UTC)

        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=next_start, end=next_end)


class USTopicsScrapyTimetable(Timetable):
    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        weekday = run_after.weekday()
        delta = timedelta(days=1)
        if weekday in (5, 6):  # Monday and Sunday -- interval is last Friday.
            days_since_friday = (run_after.weekday() - 4) % 7
            delta = timedelta(days=days_since_friday)
        else:  # Otherwise the interval is yesterday.
            delta = timedelta(days=1)


        # 1) Run After 09:25 - 10:00 (00:25 - 01:00)
        # Data Interval: 08:50 - 09:25 (23:50 - 00:25)
        if run_after >= run_after.set(hour=0, minute=25) and run_after <= run_after.set(hour=1, minute=0):
            start = (run_after-delta).set(hour=23, minute=50, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=0, minute=25, second=0).replace(tzinfo=UTC)

        # 2) Run After 10:00 - 10:35 (01:00 - 01:35)
        # Data Interval: 09:25 - 10:00 (00:25 - 01:00)
        elif run_after >= run_after.set(hour=1, minute=0) and run_after <= run_after.set(hour=1, minute=35):
            start = run_after.set(hour=0, minute=25, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=1, minute=0, second=0).replacet(tzinfo=UTC)

        # 3) Run After 10:35 - 11:10 (01:35 - 02:10)
        # Data Interval: 10:00 - 10:35 (01:00 - 01:35)
        elif run_after >= run_after.set(hour=1, minute=35) and run_after <= run_after.set(hour=2, minute=10):
            start = run_after.set(hour=1, minute=0, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=1, minute=35, second=0).replace(tzinfo=UTC)

        # 4) Run After 11:10 - 11:45 (02:10 - 02:45)
        # Data Interval: 10:35 - 11:10 (01:35 - 02:10)
        elif run_after >= run_after.set(hour=2, minute=10) and run_after <= run_after.set(hour=2, minute=45):
            start = run_after.set(hour=1, minute=35, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=2, minute=10, second=0).replace(tzinfo=UTC)

        # 5) Run After 11:45 - 12:20 (02:45 - 03:20)
        # Data Interval: 11:10 - 11:45 (02:10 - 02:45)
        elif run_after >= run_after.set(hour=2, minute=45) and run_after <= run_after.set(hour=3, minute=20):
            start = run_after.set(hour=2, minute=10, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=2, minute=45, second=0).replace(tzinfo=UTC)

        # 6) Run After 12:20 - 12:55 (03:20 - 03:55)
        # Data Interval: 11:45 - 12:20 (02:45 - 03:20)
        elif run_after >= run_after.set(hour=3, minute=20) and run_after <= run_after.set(hour=3, minute=55):
            start = run_after.set(hour=2, minute=45, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=3, minute=20, second=0).replace(tzinfo=UTC)

        # 7) Run After 12:55 - 13:30 (03:55 - 04:30)
        # Data Interval: 12:20 - 12:55 (03:20 - 03:55)
        elif run_after >= run_after.set(hour=3, minute=55) and run_after <= run_after.set(hour=4, minute=30):
            start = run_after.set(hour=3, minute=20, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=3, minute=55, second=0).replace(tzinfo=UTC)

        # 8) Run After 13:30 - 14:05 (04:30 - 05:05)
        # Data Interval: 12:55 - 13:30 (03:55 - 04:30)
        elif run_after >= run_after.set(hour=4, minute=30) and run_after <= run_after.set(hour=5, minute=5):
            start = run_after.set(hour=3, minute=55, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=4, minute=30, second=0).replace(tzinfo=UTC)

        # 9) Run After 14:05 - 14:40 (05:05 - 05:40)
        # Data Interval: 13:30 - 14:05 (04:30 - 05:05)
        elif run_after >= run_after.set(hour=5, minute=5) and run_after <= run_after.set(hour=5, minute=40):
            start = run_after.set(hour=4, minute=30, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=5, minute=5, second=0).replace(tzinfo=UTC)

        # 10) Run After 14:40 - 15:15 (05:40 - 06:15)
        # Data Interval: 14:05 - 14:40 (05:05 - 05:40)
        elif run_after >= run_after.set(hour=5, minute=40) and run_after <= run_after.set(hour=6, minute=15):
            start = run_after.set(hour=5, minute=5, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=5, minute=40, second=0).replace(tzinfo=UTC)

        # 11) Run After 15:15 - 15:50 (06:15 - 06:50)
        # Data Interval: 14:40 - 15:15 (05:40 - 06:15)
        elif run_after >= run_after.set(hour=6, minute=15) and run_after <= run_after.set(hour=6, minute=50):
            start = run_after.set(hour=5, minute=40, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=6, minute=15, second=0).replace(tzinfo=UTC)

        # 12) Run After 15:50 - 16:25 (06:50 - 07:25)
        # Data Interval: 15:15 - 15:50 (06:15 - 06:50)
        elif run_after >= run_after.set(hour=6, minute=50) and run_after <= run_after.set(hour=7, minute=25):
            start = run_after.set(hour=6, minute=15, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=6, minute=50, second=0).replace(tzinfo=UTC)

        # 13) Run After 16:25 - 08:15 (07:25 - 23:15)
        # Data Interval: 15:50 - 16:25 (06:50 - 07:25)
        elif run_after >= run_after.set(hour=7, minute=25) and run_after <= run_after.set(hour=23, minute=15):
            start = run_after.set(hour=6, minute=50, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=7, minute=25, second=0).replace(tzinfo=UTC)

        # 14) Run After 08:15 - 08:50 (23:15 - 23:50)
        # Data Interval: 16:25 - 08:15 (07:25 - 23:15)
        elif run_after >= run_after.set(hour=23, minute=15) and run_after <= run_after.set(hour=23, minute=50):
            start = run_after.set(hour=7, minute=25, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=23, minute=15, second=0).replace(tzinfo=UTC)

        # 15) Run After 08:50 - 09:00 (23:50 - 00:00)
        # Data Interval: 08:15 - 08:50 (23:15 - 23:50)
        elif run_after >= run_after.set(hour=23, minute=50) and run_after.hour <= 23:
            start = run_after.set(hour=23, minute=15, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=23, minute=50, second=0).replace(tzinfo=UTC)

        # 16) Run After 09:00 - 09:25 (00:00 - 00:25)
        # Data Interval: 08:15 - 08:50 (23:15 - 23:50)
        else:
            start = (run_after-delta).set(hour=23, minute=50, second=0).replace(tzinfo=UTC)
            end = (run_after-delta).set(hour=0, minute=25, second=0).replace(tzinfo=UTC)

        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is not None:  # There was a previous run on the regular schedule.
            last_start = last_automated_data_interval.start
            last_start_weekday = last_start.weekday()

            days_since_friday = (last_start_weekday - 4) % 7
            if 4 <= last_start_weekday <= 6:
                delta_since = timedelta(days=days_since_friday)
                delta = timedelta(days=2)
            else:
                delta_since = timedelta(days=0)
                delta = timedelta(days=0)

            # 1) 15:50 - 16:25 (06:50 - 07:25)
            # -> 16:25 - 08:15 (07:25 - 23:15)
            if last_start == last_start.set(hour=6, minute=50):
                next_start = (last_start-delta_since).set(hour=7, minute=25).replace(tzinfo=UTC)
                next_end = (last_start+delta).set(hour=23, minute=15).replace(tzinfo=UTC)

            # 2) 16:25 - 08:15 (07:25 - 23:15)
            # -> 08:15 - 08:50 (23:15 - 23:50)
            elif last_start == last_start.set(hour=7, minute=25):
                next_start = (last_start+delta).set(hour=23, minute=15).replace(tzinfo=UTC)
                next_end = (last_start+delta).set(hour=23, minute=50).replace(tzinfo=UTC)

            # 3) 08:15 - 08:50 (23:15 - 23:50)
            # -> 08:50 - 09:25 (23:50 - 00:25)
            elif last_start == last_start.set(hour=23, minute=15):
                next_start = last_start.set(hour=23, minute=50).replace(tzinfo=UTC)
                next_end = (last_start+timedelta(days=1)).set(hour=0, minute=25).replace(tzinfo=UTC)

            # 4) 08:50 - 09:25 (23:50 - 00:25)
            # -> 09:25 - 10:00 (00:25 - 01:00)
            elif last_start == last_start.set(hour=23, minute=50):
                next_start = (last_start+timedelta(days=1)).set(hour=0, minute=25).replace(tzinfo=UTC)
                next_end = (last_start+timedelta(days=1)).set(hour=1, minute=0).replace(tzinfo=UTC)

            # 5) 09:25 - 10:00 (00:25 - 01:00)
            # -> 10:00 - 10:35 (01:00 - 01:35)
            elif last_start == last_start.set(hour=0, minute=25):
                next_start = last_start.set(hour=1, minute=0).replace(tzinfo=UTC)
                next_end = last_start.set(hour=1, minute=35).replace(tzinfo=UTC)

            # 6) 10:00 - 10:35 (01:00 - 01:35)
            # -> 10:35 - 11:10 (01:35 - 02:10)
            elif last_start == last_start.set(hour=1, minute=0):
                next_start = last_start.set(hour=1, minute=35).replace(tzinfo=UTC)
                next_end = last_start.set(hour=2, minute=10).replace(tzinfo=UTC)

            # 7) 10:35 - 11:10 (01:35 - 02:10)
            # -> 11:10 - 11:45 (02:10 - 02:45)
            elif last_start == last_start.set(hour=1, minute=35):
                next_start = last_start.set(hour=2, minute=10).replace(tzinfo=UTC)
                next_end = last_start.set(hour=2, minute=45).replace(tzinfo=UTC)

            # 8) 11:10 - 11:45 (02:10 - 02:45)
            # -> 11:45 - 12:20 (02:45 - 03:20)
            elif last_start == last_start.set(hour=2, minute=10):
                next_start = last_start.set(hour=2, minute=45).replace(tzinfo=UTC)
                next_end = last_start.set(hour=3, minute=20).replace(tzinfo=UTC)

            # 9) 11:45 - 12:20 (02:45 - 03:20)
            # -> 12:20 - 12:55 (03:20 - 03:55)
            elif last_start == last_start.set(hour=2, minute=45):
                next_start = last_start.set(hour=3, minute=20).replace(tzinfo=UTC)
                next_end = last_start.set(hour=3, minute=55).replace(tzinfo=UTC)

            # 10) 12:20 - 12:55 (03:20 - 03:55)
            # ->  12:55 - 13:30 (03:55 - 04:30)
            elif last_start == last_start.set(hour=3, minute=20):
                next_start = last_start.set(hour=3, minute=55).replace(tzinfo=UTC)
                next_end = last_start.set(hour=4, minute=30).replace(tzinfo=UTC)

            # 11) 12:55 - 13:30 (03:55 - 04:30)
            # -> 13:30 - 14:05 (04:30 - 05:05)
            elif last_start == last_start.set(hour=3, minute=55):
                next_start = last_start.set(hour=4, minute=30).replace(tzinfo=UTC)
                next_end = last_start.set(hour=5, minute=5).replace(tzinfo=UTC)

            # 12) 13:30 - 14:05 (04:30 - 05:05)
            # -> 14:05 - 14:40 (05:05 - 05:40)
            elif last_start == last_start.set(hour=4, minute=30):
                next_start = last_start.set(hour=5, minute=5).replace(tzinfo=UTC)
                next_end = last_start.set(hour=5, minute=40).replace(tzinfo=UTC)

            # 13) 14:05 - 14:40 (05:05 - 05:40)
            # -> 14:40 - 15:15 (05:40 - 06:15)
            elif last_start == last_start.set(hour=5, minute=5):
                next_start = last_start.set(hour=5, minute=40).replace(tzinfo=UTC)
                next_end = last_start.set(hour=6, minute=15).replace(tzinfo=UTC)

            # 14) 14:40 - 15:15 (05:40 - 06:15)
            # -> 15:15 - 15:50 (06:15 - 06:50)
            elif last_start == last_start.set(hour=5, minute=40):
                next_start = last_start.set(hour=6, minute=15).replace(tzinfo=UTC)
                next_end = last_start.set(hour=6, minute=50).replace(tzinfo=UTC)

            # 15) 15:15 - 15:50 (06:15 - 06:50)
            # -> 15:50 - 16:25 (06:50 - 07:25)
            else:
                next_start = last_start.set(hour=6, minute=50).replace(tzinfo=UTC)
                next_end = last_start.set(hour=7, minute=25).replace(tzinfo=UTC)

        else:  # This is the first ever run on the regular schedule. First data interval will always start at 6:00 and end at 16:30
            next_start = restriction.earliest
            if next_start is None:  # No start_date. Don't schedule.
                return None
            if not restriction.catchup: # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(next_start, DateTime.combine(Date.today(), Time.min).replace(tzinfo=UTC))

#             elif next_start.time() != Time.min:
#                 # If earliest does not fall on midnight, skip to the next day.
#                 next_day = next_start.date() + timedelta(days=1)
#                 next_start = DateTime.combine(next_day, Time.min).replace(tzinfo=UTC)
            next_start_weekday = next_start.weekday()
            days_since_friday = (next_start_weekday - 4) % 7

            if 4 <= next_start_weekday <= 6:   # KST: Saturday, Sunday, Monday - Friday to Monday (UTC: Friday, Saturday, Sunday - Friday to Sunday)
                delta = timedelta(days=days_since_friday)
                next_start = (next_start-delta)
                next_end = (next_start+timedelta(days=2))
            else:
                delta = timedelta(days=1)
                next_start = (next_start-delta)
                next_end = next_start

            next_start = next_start.set(hour=7, minute=25).replace(tzinfo=UTC)
            next_end = next_end.set(hour=23, minute=15).replace(tzinfo=UTC)

        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=next_start, end=next_end)


class KREquityScrapyTimetable(Timetable):
    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        weekday = run_after.weekday()
        delta = timedelta(days=1)
        if weekday in (5, 6):  # Monday and Sunday -- interval is last Friday.
            days_since_friday = (run_after.weekday() - 4) % 7
            delta = timedelta(days=days_since_friday)
        else:  # Otherwise the interval is yesterday.
            delta = timedelta(days=1)


        # 1) Run After 09:20 - 10:00 (00:20 - 01:00)
        # Data Interval: 08:40 - 09:20 (23:40 - 00:20)
        if run_after >= run_after.set(hour=0, minute=20) and run_after <= run_after.set(hour=1, minute=0):
            start = (run_after-delta).set(hour=23, minute=40, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=0, minute=20, second=0).replace(tzinfo=UTC)

        # 2) Run After 10:00 - 10:40 (01:00 - 01:40)
        # Data Interval: 09:20 - 10:00 (00:20 - 01:00)
        elif run_after >= run_after.set(hour=1, minute=0) and run_after <= run_after.set(hour=1, minute=40):
            start = run_after.set(hour=0, minute=20, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=1, minute=0, second=0).replacet(tzinfo=UTC)

        # 3) Run After 10:40 - 11:20 (01:40 - 02:20)
        # Data Interval: 10:00 - 10:40 (01:00 - 01:40)
        elif run_after >= run_after.set(hour=1, minute=40) and run_after <= run_after.set(hour=2, minute=20):
            start = run_after.set(hour=1, minute=0, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=1, minute=40, second=0).replace(tzinfo=UTC)

        # 4) Run After 11:20 - 12:00 (02:20 - 03:00)
        # Data Interval: 10:40 - 11:20 (01:40 - 02:20)
        elif run_after >= run_after.set(hour=2, minute=20) and run_after <= run_after.set(hour=3, minute=0):
            start = run_after.set(hour=1, minute=40, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=2, minute=20, second=0).replace(tzinfo=UTC)

        # 5) Run After 12:00 - 12:40 (03:00 - 03:40)
        # Data Interval: 11:20 - 12:00 (02:20 - 03:00)
        elif run_after >= run_after.set(hour=3, minute=0) and run_after <= run_after.set(hour=3, minute=40):
            start = run_after.set(hour=2, minute=20, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=3, minute=0, second=0).replace(tzinfo=UTC)

        # 6) Run After 12:40 - 13:20 (03:40 - 04:20)
        # Data Interval: 12:00 - 12:40 (03:00 - 03:40)
        elif run_after >= run_after.set(hour=3, minute=40) and run_after <= run_after.set(hour=4, minute=20):
            start = run_after.set(hour=3, minute=0, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=3, minute=40, second=0).replace(tzinfo=UTC)

        # 7) Run After 13:20 - 14:00 (04:20 - 05:00)
        # Data Interval: 12:40 - 13:20 (03:40 - 04:20)
        elif run_after >= run_after.set(hour=4, minute=20) and run_after <= run_after.set(hour=5, minute=0):
            start = run_after.set(hour=3, minute=40, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=4, minute=20, second=0).replace(tzinfo=UTC)

        # 8) Run After 14:00 - 14:40 (05:00 - 05:40)
        # Data Interval: 13:20 - 14:00 (04:20 - 05:00)
        elif run_after >= run_after.set(hour=5, minute=0) and run_after <= run_after.set(hour=5, minute=40):
            start = run_after.set(hour=4, minute=20, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=5, minute=0, second=0).replace(tzinfo=UTC)

        # 9) Run After 14:40 - 15:20 (05:40 - 06:20)
        # Data Interval: 14:00 - 14:40 (05:00 - 05:40)
        elif run_after >= run_after.set(hour=5, minute=40) and run_after <= run_after.set(hour=6, minute=20):
            start = run_after.set(hour=5, minute=0, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=5, minute=40, second=0).replace(tzinfo=UTC)

        # 10) Run After 15:20 - 16:00 (06:20 - 07:00)
        # Data Interval: 14:40 - 15:20 (05:40 - 06:20)
        elif run_after >= run_after.set(hour=6, minute=20) and run_after <= run_after.set(hour=7, minute=0):
            start = run_after.set(hour=5, minute=40, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=6, minute=20, second=0).replace(tzinfo=UTC)

        # 11) Run After 16:00 - 08:00 (07:00 - 23:00)
        # Data Interval: 15:20 - 16:00 (06:20 - 07:00)
        elif run_after >= run_after.set(hour=7, minute=0) and run_after <= run_after.set(hour=23, minute=0):
            start = run_after.set(hour=6, minute=20, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=7, minute=0, second=0).replace(tzinfo=UTC)

        # 12) Run After 08:00 - 08:40 (23:00 - 23:50)
        # Data Interval: 16:00 - 08:00 (07:00 - 23:00)
        elif run_after >= run_after.set(hour=23, minute=0) and run_after <= run_after.set(hour=23, minute=40):
            start = run_after.set(hour=7, minute=0, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=23, minute=0, second=0).replace(tzinfo=UTC)

        # 13) Run After 08:40 - 09:00 (23:40 - 00:00)
        # Data Interval: 08:00 - 08:40 (23:00 - 23:40)
        elif run_after >= run_after.set(hour=23, minute=0) and run_after.hour <= 23:
            start = run_after.set(hour=23, minute=0, second=0).replace(tzinfo=UTC)
            end = run_after.set(hour=23, minute=40, second=0).replace(tzinfo=UTC)

        # 14) Run After 09:00 - 09:20 (00:00 - 00:20)
        # Data Interval: 08:00 - 08:40 (23:00 - 23:40)
        else:
            start = (run_after-delta).set(hour=23, minute=0, second=0).replace(tzinfo=UTC)
            end = (run_after-delta).set(hour=23, minute=40, second=0).replace(tzinfo=UTC)

        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is not None:  # There was a previous run on the regular schedule.
            last_start = last_automated_data_interval.start
            last_start_weekday = last_start.weekday()

            days_since_friday = (last_start_weekday - 4) % 7
            if 4 <= last_start_weekday <= 6:
                delta_since = timedelta(days=days_since_friday)
                delta = timedelta(days=2)
            else:
                delta_since = timedelta(days=0)
                delta = timedelta(days=0)

            # 1) 15:20 - 16:00 (06:20 - 07:00)
            # -> 16:00 - 08:00 (07:00 - 23:00)
            if last_start == last_start.set(hour=6, minute=20):
                next_start = (last_start-delta_since).set(hour=7, minute=0).replace(tzinfo=UTC)
                next_end = (last_start+delta).set(hour=23, minute=0).replace(tzinfo=UTC)

            # 2) 16:00 - 08:00 (07:00 - 23:00)
            # -> 08:00 - 08:40 (23:00 - 23:40)
            elif last_start == last_start.set(hour=7, minute=00):
                next_start = (last_start+delta).set(hour=23, minute=00).replace(tzinfo=UTC)
                next_end = (last_start+delta).set(hour=23, minute=40).replace(tzinfo=UTC)

            # 3) 08:00 - 08:40 (23:00 - 23:40)
            # -> 08:40 - 09:20 (23:40 - 00:20)
            elif last_start == last_start.set(hour=23, minute=00):
                next_start = last_start.set(hour=23, minute=40).replace(tzinfo=UTC)
                next_end = (last_start+timedelta(days=1)).set(hour=0, minute=20).replace(tzinfo=UTC)

            # 4) 08:40 - 09:20 (23:40 - 00:20)
            # -> 09:20 - 10:00 (00:20 - 01:00)
            elif last_start == last_start.set(hour=23, minute=40):
                next_start = (last_start+timedelta(days=1)).set(hour=0, minute=20).replace(tzinfo=UTC)
                next_end = (last_start+timedelta(days=1)).set(hour=1, minute=0).replace(tzinfo=UTC)

            # 5) 09:20 - 10:00 (00:20 - 01:00)
            # -> 10:00 - 10:40 (01:00 - 01:40)
            elif last_start == last_start.set(hour=0, minute=20):
                next_start = last_start.set(hour=1, minute=0).replace(tzinfo=UTC)
                next_end = last_start.set(hour=1, minute=40).replace(tzinfo=UTC)

            # 6) 10:00 - 10:40 (01:00 - 01:40)
            # -> 10:40 - 11:20 (01:40 - 02:20)
            elif last_start == last_start.set(hour=1, minute=0):
                next_start = last_start.set(hour=1, minute=40).replace(tzinfo=UTC)
                next_end = last_start.set(hour=2, minute=20).replace(tzinfo=UTC)

            # 7) 10:40 - 11:20 (01:40 - 02:20)
            # -> 11:20 - 12:00 (02:20 - 03:00)
            elif last_start == last_start.set(hour=1, minute=40):
                next_start = last_start.set(hour=2, minute=20).replace(tzinfo=UTC)
                next_end = last_start.set(hour=3, minute=0).replace(tzinfo=UTC)

            # 8) 11:20 - 12:00 (02:20 - 03:00)
            # -> 12:00 - 12:40 (03:00 - 03:40)
            elif last_start == last_start.set(hour=2, minute=20):
                next_start = last_start.set(hour=3, minute=00).replace(tzinfo=UTC)
                next_end = last_start.set(hour=3, minute=40).replace(tzinfo=UTC)

            # 9) 12:00 - 12:40 (03:00 - 03:40)
            # -> 12:40 - 13:20 (03:40 - 04:20)
            elif last_start == last_start.set(hour=3, minute=0):
                next_start = last_start.set(hour=3, minute=40).replace(tzinfo=UTC)
                next_end = last_start.set(hour=4, minute=20).replace(tzinfo=UTC)

            # 10) 12:40 - 13:20 (03:40 - 04:20)
            # -> 13:20 - 14:00 (04:20 - 05:00)
            elif last_start == last_start.set(hour=3, minute=40):
                next_start = last_start.set(hour=4, minute=20).replace(tzinfo=UTC)
                next_end = last_start.set(hour=5, minute=0).replace(tzinfo=UTC)

            # 11) 13:20 - 14:00 (04:20 - 05:00)
            # -> 14:00 - 14:40 (05:00 - 05:40)
            elif last_start == last_start.set(hour=4, minute=20):
                next_start = last_start.set(hour=5, minute=0).replace(tzinfo=UTC)
                next_end = last_start.set(hour=5, minute=40).replace(tzinfo=UTC)

            # 12) 14:00 - 14:40 (05:00 - 05:40)
            # -> 14:40 - 15:20 (05:40 - 06:20)
            elif last_start == last_start.set(hour=5, minute=0):
                next_start = last_start.set(hour=5, minute=40).replace(tzinfo=UTC)
                next_end = last_start.set(hour=6, minute=20).replace(tzinfo=UTC)

            # 13) 14:40 - 15:20 (05:40 - 06:20)
            # -> 15:20 - 16:00 (06:20 - 07:00)
            else:
                next_start = last_start.set(hour=6, minute=20).replace(tzinfo=UTC)
                next_end = last_start.set(hour=7, minute=00).replace(tzinfo=UTC)

        else:  # This is the first ever run on the regular schedule. First data interval will always start at 6:00 and end at 16:30
            next_start = restriction.earliest
            if next_start is None:  # No start_date. Don't schedule.
                return None
            if not restriction.catchup: # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(next_start, DateTime.combine(Date.today(), Time.min).replace(tzinfo=UTC))

#             elif next_start.time() != Time.min:
#                 # If earliest does not fall on midnight, skip to the next day.
#                 next_day = next_start.date() + timedelta(days=1)
#                 next_start = DateTime.combine(next_day, Time.min).replace(tzinfo=UTC)
            next_start_weekday = next_start.weekday()
            days_since_friday = (next_start_weekday - 4) % 7

            if 4 <= next_start_weekday <= 6:   # KST: Saturday, Sunday, Monday - Friday to Monday (UTC: Friday, Saturday, Sunday - Friday to Sunday)
                delta = timedelta(days=days_since_friday)
                next_start = (next_start-delta)
                next_end = (next_start+timedelta(days=2))
            else:
                delta = timedelta(days=1)
                next_start = (next_start-delta)
                next_end = next_start

            next_start = next_start.set(hour=7, minute=0).replace(tzinfo=UTC)
            next_end = next_end.set(hour=23, minute=0).replace(tzinfo=UTC)

        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=next_start, end=next_end)
        
        
class USEquityScrapyTimetablePlugin(AirflowPlugin):
    name = "US_equity_scrapy_timetable_plugin"
    timetables = [USEquityScrapyTimetable]

class USTopicsScrapyTimetablePlugin(AirflowPlugin):
    name = "US_topics_scrapy_timetable_plugin"
    timetables = [USTopicsScrapyTimetable]

class KREquityScrapyTimetablePlugin(AirflowPlugin):
    name = "KR_equity_scrapy_timetable_plugin"
    timetables = [KREquityScrapyTimetable]