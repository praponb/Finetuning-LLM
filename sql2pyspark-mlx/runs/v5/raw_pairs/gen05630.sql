select
    id,
    customer_id,
    browser,
    started_at,
    country
from (
    select *, row_number() over (partition by country order by started_at desc nulls first, id asc) as rank_in_group
    from sessions
) ranked
where rank_in_group = 1;
