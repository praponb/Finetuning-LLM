with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        stock_qty * price as stock_qty_times_price,
        coalesce(brand, 'OTHER') as resolved_brand
    from products
    where month(launch_date) in (3, 7, 9)
      and brand is not null
),
with_last as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.last_restock,
        x.quantity
    from valid s
    join lateral (
            select c.last_restock, c.quantity
            from inventory c
            where c.product_id = s.id
              and c.last_restock <= s.launch_date
            order by c.last_restock desc, c.id desc
            limit 1
        ) x on true
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        tags,
        resolved_brand,
        row_number() over (partition by category order by last_restock desc, id) as event_seq,
        sum(quantity) over (partition by supplier_id order by stock_qty_times_price, id rows between unbounded preceding and current row) as cumulative_quantity,
        max(price) over (partition by supplier_id) as supplier_id_max_price
    from with_last
    where brand in ('Initech', 'Acme')
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from store_sales ch
            where ch.product_id = s.id
              and ch.sale_date < s.launch_date
        )
)
-- Running and moving aggregates
select
    id,
    category,
    brand,
    cost,
    launch_date,
    tags,
    event_seq,
    avg(supplier_id_max_price) over (partition by category, brand) as category_avg_supplier_id_max_price,
    avg(stock_qty) over w_running as stock_qty_avg_6_prior,
    cume_dist() over w_recent as supplier_id_max_price_cume_dist,
    round(supplier_id_max_price / sum(supplier_id_max_price) over (partition by category, brand), 4) as supplier_id_max_price_share,
    avg(cost) over w_hist as cost_avg_2
from not_flagged
window w_running as (partition by category, brand order by cost, id rows between 6 preceding and 1 preceding), w_recent as (order by supplier_id_max_price), w_hist as (partition by brand, category order by cost, id rows between 2 preceding and current row)
