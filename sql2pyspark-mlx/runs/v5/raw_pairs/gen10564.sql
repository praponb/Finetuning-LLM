with ranked as (
    -- Keep one row per key (latest first)
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft
    from stores
    qualify row_number() over (partition by city order by sqft desc, id) = 1
),
with_latest as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.region,
        s.sqft,
        top1.discount,
        top1.order_shipping_fee
    from ranked s
    left join lateral (
            select c.discount, c.shipping_fee as order_shipping_fee
            from orders c
            where c.store_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) top1 on true
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, trim(region) as clean_region
    from with_latest
    where region is not null
      and region is distinct from 'Central'
),
latest_match as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.region,
        s.discount,
        x.status
    from base s
    left join lateral (
            select c.status
            from orders c
            where c.store_id = s.id
            order by c.order_date desc, c.id desc
            limit 1
        ) x on true
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from latest_match s
    where exists (
            select 1
            from orders c
            where c.store_id = s.id
              and c.order_date between '2023-09-01' and '2025-06-01'
        )
)
-- Multi-level aggregation
select
    region,
    case when discount >= 0.44 then 'LARGE' else 'SMALL' end as discount_tier,
    approx_percentile(discount, array(0.25, 0.5, 0.75)) as discount_quartiles,
    count(distinct region) as distinct_regions,
    sort_array(collect_set(region)) as region_list,
    count(*) filter (where region = 'Central') as central_rows
from kept
group by region, discount_tier
order by region desc
