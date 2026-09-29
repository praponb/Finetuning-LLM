select
    id,
    status,
    start_date,
    end_date
from (
    select
        id,
        status,
        start_date,
        end_date,
        row_number() over (partition by status order by start_date, id) rn
    from subscriptions
) t
where rn <= 3
