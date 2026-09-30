with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        store_name,
        region,
        opened_date,
        sqft,
        coalesce(sqft, 0) as sqft_filled,
        coalesce(region, 'DIRECT') as resolved_region
    from stores
    where region is not null
),
with_last as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.store_name,
        s.sqft,
        s.resolved_region,
        latest.order_discount
    from cleaned s
    left join lateral (
            select c.discount as order_discount
            from orders c
            where c.store_id = s.id
              and c.order_date <= s.opened_date
            order by c.order_date desc, c.id desc
            limit 1
        ) latest on true
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_last s
    where not exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.store_id = 7
        )
),
filtered as (
    -- Keep recent, valid rows
    select *, case when order_discount >= 0.32 then 'High' when order_discount >= 0.16 then 'Medium' else 'Low' end as order_discount_level
    from eligible
    where lower(trim(store_name)) = 'downtown express'
),
enriched as (
    -- Ranking and gap features
    select
        id,
        sqft,
        resolved_region,
        order_discount,
        order_discount_level,
        ntile(5) over (order by sqft desc, id) as sqft_bucket,
        avg(order_discount) over (order by order_discount, id rows between 3 preceding and 1 preceding) as order_discount_avg_3_prior,
        avg(sqft) over (order by order_discount, id rows between 3 preceding and current row) as sqft_avg_3,
        avg(sqft) over (order by sqft, id rows between 6 preceding and current row) as sqft_avg_6
    from filtered
    where lower(trim(store_name)) = 'airport kiosk'
),
filtered_stores as (
    -- Keep recent, valid rows
    select
        id,
        sqft,
        resolved_region,
        order_discount,
        order_discount_level,
        sqft_bucket,
        order_discount_avg_3_prior,
        sqft_avg_3,
        sqft_avg_6,
        round(sqft / sqft_bucket, 2) as sqft_per_sqft_bucket,
        cast(order_discount as int) as order_discount_int
    from enriched
    where sqft is not null
),
eligible_stores as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered_stores s
    where not exists (
            select 1
            from orders c
            where c.store_id = s.id
              and c.status = 'PENDING'
        )
)
-- Running and moving aggregates
select
    id,
    sqft,
    resolved_region,
    sqft_bucket,
    order_discount_avg_3_prior,
    sqft_avg_3,
    sqft_avg_6,
    sqft_per_sqft_bucket,
    ntile(3) over (order by sqft_avg_3 desc, id) as sqft_avg_3_bucket,
    avg(order_discount_int) over (order by sqft_avg_6, id rows between 3 preceding and 1 preceding) as order_discount_int_avg_3_prior,
    sum(sqft_avg_3) over (order by sqft_per_sqft_bucket, id rows between unbounded preceding and current row) as running_sqft_avg_3,
    avg(sqft_avg_6) over (order by sqft_avg_3, id rows between 5 preceding and current row) as sqft_avg_6_avg_5
from eligible_stores
