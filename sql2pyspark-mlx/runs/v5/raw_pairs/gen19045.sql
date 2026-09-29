with enriched as (
    select
        id,
        sqft,
        sum(sqft) over (partition by region order by sqft, id rows between unbounded preceding and current row) cumulative_sqft,
        row_number() over (partition by city order by sqft desc, id) seq_no
    from stores
    where city in ('Bangkok', 'Singapore')
),
with_latest as (
    select
        s.id,
        s.sqft,
        latest.store_sale_revenue
    from enriched s
    left join lateral (
            select c.revenue as store_sale_revenue
            from store_sales c
            where c.store_id = s.id
            order by c.units desc, c.id desc
            limit 1
        ) latest on true
),
valid as (
    select
        *,
        round(store_sale_revenue * 0.9, 2) adjusted_store_sale_revenue,
        case when store_sale_revenue >= 3400 then 'High' when store_sale_revenue >= 1200 then 'Medium' else 'Low' end store_sale_revenue_band
    from with_latest
    where store_sale_revenue is not null
),
kept as (
    select s.*
    from valid s
    where not exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.discount < 0.27
        )
),
kept_stores as (
    select s.*
    from kept s
    where not exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.store_id in (10, 22)
        )
),
with_latest_stores as (
    select
        s.id,
        s.sqft,
        s.adjusted_store_sale_revenue,
        s.store_sale_revenue_band,
        top1.store_sale_revenue_2
    from kept_stores s
    join lateral (
            select c.revenue as store_sale_revenue_2
            from store_sales c
            where c.store_id = s.id
            order by c.units desc, c.id desc
            limit 1
        ) top1 on true
),
latest_match as (
    select s.*, x.discount
    from with_latest_stores s
    join lateral (
            select c.discount
            from orders c
            where c.store_id = s.id
            order by c.order_date desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    adjusted_store_sale_revenue,
    store_sale_revenue_2,
    sum(store_sale_revenue_2) over (order by discount, id rows between unbounded preceding and current row) running_store_sale_revenue_2,
    ntile(5) over (order by adjusted_store_sale_revenue, id) adjusted_store_sale_revenue_bucket
from latest_match;
