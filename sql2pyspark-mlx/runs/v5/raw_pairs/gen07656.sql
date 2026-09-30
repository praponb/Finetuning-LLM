with enriched as (
    -- Running and moving aggregates
    select
        *,
        count(revenue) over (partition by product_id, store_id) as product_id_count_revenue,
        sum(units) over (order by sale_date, id rows between unbounded preceding and current row) as cumulative_units,
        round(revenue / sum(revenue) over (partition by product_id), 4) as revenue_share,
        sum(units) over (partition by store_id order by sale_date, id rows between unbounded preceding and current row) as running_units,
        corr(revenue, units) over w_part as corr_revenue_units
    from store_sales
    window w_part as (partition by product_id)
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        product_id,
        sale_date,
        cumulative_units,
        revenue_share,
        corr_revenue_units,
        revenue_share - revenue as revenue_share_minus_revenue
    from enriched
    where units <> 18
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        product_id,
        sale_date,
        cumulative_units,
        revenue_share,
        revenue_share_minus_revenue,
        revenue_share - revenue_share_minus_revenue as revenue_share_minus_revenue_share_minus_revenue
    from base
    where product_id in (9, 10)
),
joined as (
    -- Attach products attributes
    select s.*, p.cost
    from cleaned s
    left join products p
        on s.product_id = p.id
),
scored as (
    -- Running and moving aggregates
    select
        *,
        sum(cost) over (partition by product_id order by revenue_share_minus_revenue_share_minus_revenue, id rows between unbounded preceding and current row) as cumulative_cost,
        avg(revenue_share_minus_revenue) over (partition by product_id order by revenue_share, id rows between 5 preceding and current row) as revenue_share_minus_revenue_avg_5,
        avg(revenue_share) over (partition by product_id order by revenue_share_minus_revenue, id rows between 6 preceding and 1 preceding) as revenue_share_avg_6_prior
    from joined
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by product_id order by revenue_share_minus_revenue_share_minus_revenue desc nulls last, id) as rn
    from scored
    qualify rn = 1
)
select
    s.id,
    s.product_id,
    s.sale_date,
    s.cumulative_units,
    s.revenue_share,
    s.revenue_share_minus_revenue_share_minus_revenue,
    s.cost,
    s.revenue_share_minus_revenue_avg_5,
    s.revenue_share_avg_6_prior,
    (select avg(x.unit_price) from order_items x where x.product_id = s.product_id) as avg_order_item_unit_price
from latest s
