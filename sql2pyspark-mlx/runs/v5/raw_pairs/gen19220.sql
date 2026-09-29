select *
from (
    select
        id,
        browser,
        started_at,
        rank() over (partition by browser order by started_at desc) as rnk
    from sessions
) as ranked
where rnk <= 5;
