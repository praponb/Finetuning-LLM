with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        last_day(launch_date) as launch_date_month_end,
        coalesce(brand, 'NONE') as resolved_brand
    from products
    where brand is not null
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        product_name,
        price,
        cost,
        stock_qty,
        supplier_id,
        resolved_brand,
        count(stock_qty) over w_hist as last3_count_stock_qty,
        sum(stock_qty) over w_hist as last3_sum_stock_qty,
        min(stock_qty) over w_hist as last3_min_stock_qty,
        avg(stock_qty) over (partition by brand order by price, id rows between 5 preceding and current row) as stock_qty_avg_5
    from cleaned
    window w_hist as (partition by brand, category order by stock_qty, id rows between 2 preceding and current row)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows s
    where not exists (
            select 1
            from store_sales ch
            where ch.product_id = s.id
              and ch.sale_date <= '2025-04-01'
        )
),
scored as (
    -- Running and moving aggregates
    select
        *,
        rank() over w_running as stock_qty_avg_5_rank,
        lead(price, 2) over w_hist as next_price,
        sum(cost) over (partition by supplier_id order by stock_qty_avg_5, id rows between unbounded preceding and current row) as cumulative_cost
    from kept
    where stock_qty_avg_5 between 205 and 280
    window w_running as (order by stock_qty_avg_5 desc nulls last), w_hist as (order by last3_count_stock_qty, id)
),
with_dims as (
    -- Attach suppliers attributes
    select
        e.*,
        p.supplier_name,
        p.rating,
        p.country
    from scored e
    left join suppliers p
        on e.supplier_id <=> p.id
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_dims s
    where not exists (
            select 1
            from order_items x
            where x.product_id = s.id
              and x.order_id = 25
        )
),
totals as (
    -- Multi-level aggregation
    select
        country,
        count_if(country = 'TH') as th_events,
        round(sum(last3_min_stock_qty) / nullif(count(case when country = 'JP' then 1 end), 0), 2) as last3_min_stock_qty_per_jp,
        bit_and(stock_qty) as stock_qty_bit_and
    from eligible
    group by country
)
select country, last3_min_stock_qty_per_jp
from totals
