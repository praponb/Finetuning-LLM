select id, start_date, end_date
from subscriptions
where end_date > '2024-10-01'
union
select id, start_date, end_date
from subscriptions
where monthly_fee between 90 and 240
order by id;
