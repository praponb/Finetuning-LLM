with remaining_keys as (
    -- Set difference on keys
    select region, id
    from stores
    except all
    select region, id
    from stores
    where opened_date <= '2025-05-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from stores s
    inner join remaining_keys k on s.region = k.region and s.id = k.id
)
-- Grouped metrics
select
    date_trunc('week', opened_date) as week_start,
    city,
    round(sum(sqft) / nullif(count(case when region = 'West' then 1 end), 0), 2) as sqft_per_west,
    sum(case when city = 'Chiang Mai' then sqft else 0 end) as chiang_mai_sqft,
    bit_and(sqft) as sqft_bit_and,
    round(sum(sqft) / nullif(count(case when city = 'Berlin' then 1 end), 0), 2) as sqft_per_berlin,
    count(*) filter (where region = 'West') as west_rows
from kept_rows
group by week_start, city
order by sqft_per_west nulls last
