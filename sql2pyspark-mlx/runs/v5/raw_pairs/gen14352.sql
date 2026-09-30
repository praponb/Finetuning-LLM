with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        round(price, 1) as price_rounded
    from products
    where cost > 720
),
joined as (
    -- Attach suppliers attributes
    select src.*, b.supplier_name
    from cleaned src
    join suppliers b
        on src.supplier_id = b.id
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from joined s
    where not exists (
            select 1
            from store_sales ch
            where ch.product_id = s.id
              and ch.sale_date >= s.launch_date
        )
),
eligible_products as (
    -- Anti-join via a correlated subquery
    select s.*
    from eligible s
    where not exists (
            select 1
            from store_sales ch
            where ch.product_id = s.id
              and ch.sale_date <= s.launch_date
        )
),
joined_products as (
    -- Attach suppliers attributes
    select src.*, d.country
    from eligible_products src
    left join suppliers d
        on src.supplier_id = d.id
)
-- Ranking and gap features
select
    *,
    max(cost) over w_hist as last3_max_cost,
    avg(cost) over w_hist as last3_avg_cost,
    sum(cost) over w_hist as last3_sum_cost
from joined_products
where month(launch_date) in (4, 6, 9)
window w_hist as (partition by supplier_id order by stock_qty, id rows between 2 preceding and current row);
