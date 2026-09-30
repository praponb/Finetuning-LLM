select id, country, started_at
from (
    select
        id,
        country,
        started_at,
        rank() over (partition by country order by started_at desc) as row_num
    from sessions
) as ranked
where row_num = 1;
