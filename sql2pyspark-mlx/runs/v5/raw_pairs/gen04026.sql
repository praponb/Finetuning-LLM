select
    id,
    device,
    browser
from (
    select *, row_number() over (partition by browser order by started_at desc, id) as rn
    from sessions
) t
where rn = 1;
