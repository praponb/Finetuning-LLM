select id, start_date, plan
from subscriptions
where start_date = (select max(start_date) from subscriptions where end_date >= '2025-02-16');
