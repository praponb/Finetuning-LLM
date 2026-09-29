select id, plan, start_date
from (
    select
        id,
        plan,
        start_date,
        row_number() over (partition by plan order by start_date desc, id) as rnk
    from subscriptions
) as t
where rnk = 1;
