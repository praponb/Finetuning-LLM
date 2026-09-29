select
    id,
    start_date,
    end_date,
    status
from subscriptions
where not (start_date < '2023-05-27' or month(end_date) in (3, 7, 8))
