with valid as (
    -- Keep recent, valid rows
    select
        *,
        length(store_name) as store_name_length,
        coalesce(region, 'UNKNOWN') as resolved_region
    from stores
    where region is not null
),
latest_match as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        s.region,
        s.opened_date,
        s.sqft,
        s.store_name_length,
        top1.units,
        top1.sale_date
    from valid s
    join lateral (
            select c.units, c.sale_date
            from store_sales c
            where c.store_id = s.id
              and c.sale_date <= s.opened_date
            order by c.sale_date desc, c.id desc
            limit 1
        ) top1 on true
),
with_latest as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.store_sale_units,
        top1.store_sale_revenue
    from latest_match s
    left join lateral (
            select c.units as store_sale_units, c.revenue as store_sale_revenue
            from store_sales c
            where c.store_id = s.id
              and c.sale_date <= s.sale_date
            order by c.sale_date desc, c.id desc
            limit 1
        ) top1 on true
),
valid_stores as (
    -- Basic cleansing and defaults
    select
        *,
        store_sale_units * sqft as store_sale_units_times_sqft,
        coalesce(region, 'N/A') as region_filled
    from with_latest
    where opened_date > date '2023-07-19'
      and region is not null
),
metrics as (
    -- Ranking and gap features
    select
        id,
        sqft,
        store_name_length,
        units,
        store_sale_units,
        store_sale_revenue,
        store_sale_units_times_sqft,
        region_filled,
        lead(store_sale_units_times_sqft) over w_recent as next_store_sale_units_times_sqft,
        lead(units) over w_recent as next_units,
        lag(units) over w_recent as prev_units,
        row_number() over w_recent as row_seq,
        last_value(store_sale_units_times_sqft, true) over (partition by city order by store_sale_revenue, id rows between unbounded preceding and current row) as last_known_store_sale_units_times_sqft
    from valid_stores
    where city = 'Berlin'
    window w_recent as (partition by region order by store_sale_revenue, id)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics s
    where not exists (
            select 1
            from orders c
            where c.store_id = s.id
              and c.channel in ('web', 'store')
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.shipping_fee <= 19
        )
)
-- Aggregate with conditional metrics
select
    case when units >= 37 then 'HIGH' else 'LOW' end as units_tier,
    case when store_sale_units >= 41 then 'HIGH' else 'LOW' end as store_sale_units_tier,
    count(distinct id) as distinct_ids,
    avg(store_name_length) as avg_store_name_length,
    max(store_sale_units_times_sqft) as max_store_sale_units_times_sqft
from not_flagged
group by case when units >= 37 then 'HIGH' else 'LOW' end, case when store_sale_units >= 41 then 'HIGH' else 'LOW' end
order by units_tier desc nulls last;
