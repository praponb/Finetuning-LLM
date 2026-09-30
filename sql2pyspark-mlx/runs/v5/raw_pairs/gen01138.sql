with remaining_keys as (
    select customer_id, browser
    from sessions
    intersect all
    select customer_id, browser
    from sessions
    where country in ('US', 'TH', 'JP')
),
kept_rows as (
    select s.*
    from sessions s
    inner join remaining_keys k on s.customer_id = k.customer_id and s.browser = k.browser
)
select
    id,
    device,
    browser,
    started_at,
    country,
    case when browser rlike '^(Edge|Firefox)$' then 'CORE' else 'EXTENDED' end as browser_zone
from kept_rows
order by started_at desc nulls last
