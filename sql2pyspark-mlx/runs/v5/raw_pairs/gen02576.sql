select end_date as event_date
from subscriptions
union all
select opened_date as event_date
from stores
where month(opened_date) in (1, 3, 4)
order by event_date;
