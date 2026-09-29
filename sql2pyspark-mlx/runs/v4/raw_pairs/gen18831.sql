select *
from (
    select *, rank() over (partition by status order by end_date desc) as pos
    from subscriptions
) as ranked
where pos <= 3;
