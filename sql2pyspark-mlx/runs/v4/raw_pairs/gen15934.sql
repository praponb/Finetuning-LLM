with base as (
    -- Keep recent, valid rows
    select
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        lower(product_name) product_name_lower,
        datediff(current_date(), launch_date) days_since_launch_date,
        coalesce(brand, 'N/A') brand_clean
    from products
    where launch_date > to_date('2024-02-22')
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        product_name_lower,
        days_since_launch_date,
        brand_clean,
        avg(stock_qty) over (partition by brand order by days_since_launch_date, id rows between 5 preceding and 1 preceding) stock_qty_avg_5_prior,
        avg(cost) over w_grp cost_avg_6
    from base
    window w_grp as (partition by supplier_id order by cost, id rows between 6 preceding and current row)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from scored s
    where not exists (
            select 1
            from inventory x
            where x.product_id = s.id
              and year(x.last_restock) >= 2024
        )
),
enriched as (
    -- Attach suppliers attributes
    select
        a.*,
        b.rating,
        b.supplier_name
    from kept a
    inner join suppliers b
        on a.supplier_id = b.id
),
with_last as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.inventory_quantity,
        top1.last_restock
    from enriched s
    left join lateral (
            select c.quantity as inventory_quantity, c.last_restock
            from inventory c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        concat(brand, ' ', product_name) brand_product_name,
        last_day(launch_date) launch_date_month_end,
        coalesce(brand, 'N/A') resolved_brand
    from with_last
    where launch_date >= date '2023-09-02'
      and year(launch_date) = 2024
)
-- Grouped metrics
select
    brand,
    date_trunc('year', launch_date_month_end) as year_start,
    sum(case when brand = 'Acme' then days_since_launch_date else 0 end) as acme_days_since_launch_date,
    round(avg(stock_qty_avg_5_prior), 2) as avg_stock_qty_avg_5_prior,
    percentile_approx(cost_avg_6, 0.95) as cost_avg_6_p95,
    count_if(brand = 'Globex') as globex_events
from filtered
group by brand, date_trunc('year', launch_date_month_end)
order by globex_events desc nulls first;
