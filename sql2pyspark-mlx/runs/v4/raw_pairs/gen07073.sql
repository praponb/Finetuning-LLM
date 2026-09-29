select id, browser, started_at
from (
    select
        id,
        browser,
        started_at,
        row_number() over (partition by browser order by started_at desc, id) as rn
    from sessions
    where country is null
) as r
where rn <= 3;
