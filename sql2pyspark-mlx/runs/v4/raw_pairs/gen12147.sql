select id, end_date, rank() over (order by end_date) as rnk
from subscriptions;
