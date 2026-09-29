with scored as (
    -- Running and moving aggregates
    select
        *,
        min(sqft) over (partition by city) as city_min_sqft,
        row_number() over (partition by city order by opened_date, id) as event_seq
    from stores
),
with_latest as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.store_sale_sale_date,
        top1.store_sale_units
    from scored s
    left join lateral (
            select c.sale_date as store_sale_sale_date, c.units as store_sale_units
            from store_sales c
            where c.store_id = s.id
            order by c.revenue desc, c.id desc
            limit 1
        ) top1 on true
),
metrics as (
    -- Ranking and gap features
    select
        id,
        store_name,
        city,
        region,
        sqft,
        city_min_sqft,
        store_sale_units,
        lead(store_sale_units, 1) over (partition by region order by store_sale_units, id) as next_store_sale_units,
        row_number() over (order by store_sale_sale_date, id) as event_seq_2
    from with_latest
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        ceil(next_store_sale_units) as next_store_sale_units_whole,
        round(city_min_sqft * 1.07, 2) as adjusted_city_min_sqft,
        coalesce(region, 'UNKNOWN') as region_filled
    from metrics
    where region is distinct from 'North'
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from cleaned s
    where not exists (
            select 1
            from store_sales c
            where c.store_id = s.id
              and c.revenue between 2100 and 2500
        )
)
select
    id,
    store_name,
    city,
    region,
    sqft,
    next_store_sale_units,
    event_seq_2,
    region_filled
from eligible
order by next_store_sale_units desc nulls first;
