with filtered as (
    -- Basic cleansing and defaults
    select *, coalesce(segment, 'N/A') segment_filled
    from customers
    where segment is not null
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered s
    where not exists (
            select 1
            from sessions c
            where c.customer_id = s.id
              and c.device != 'mobile'
        )
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        name,
        email,
        is_active,
        age,
        segment_filled
    from eligible
    qualify rank() over (partition by city order by age desc) = 1
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from ranked s
    where not exists (
            select 1
            from sessions ch
            where ch.customer_id = s.id
              and ch.browser in ('Safari', 'Chrome', 'Edge')
        )
)
-- Grouped metrics
select
    case when age >= 63 then 'HIGH' else 'LOW' end as age_tier,
    avg(age) as avg_age,
    round(avg(age), 2) as avg_age_2,
    approx_percentile(age, 0.5) as age_p50
from kept
group by case when age >= 63 then 'HIGH' else 'LOW' end
having avg_age_2 > 0
order by age_p50 desc;
