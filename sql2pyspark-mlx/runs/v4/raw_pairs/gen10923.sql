select id, plan, date_sub(start_date, 80) as start_date_minus_80d
from subscriptions;
