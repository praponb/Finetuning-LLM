with with_latest as (
    select s.*, top1.store_sale_revenue
    from products s
    left join lateral (
            select c.revenue as store_sale_revenue
            from store_sales c
            where c.product_id = s.id
            order by c.sale_date desc, c.id desc
            limit 1
        ) top1 on true
),
scored as (
    select
        *,
        row_number() over w_hist as seq_no,
        percent_rank() over (partition by category order by store_sale_revenue desc nulls first) as store_sale_revenue_percent_rank,
        stddev_samp(store_sale_revenue) over (partition by brand, category) as brand_stddev_samp_store_sale_revenue,
        corr(stock_qty, price) over (partition by category) as corr_stock_qty_price
    from with_latest
    window w_hist as (partition by brand order by price, id)
),
metrics as (
    select
        *,
        lead(brand_stddev_samp_store_sale_revenue, 1, 0) over (partition by supplier_id, brand order by stock_qty, id) as next_brand_stddev_samp_store_sale_revenue,
        last_value(store_sale_revenue, true) over (order by launch_date, id rows between unbounded preceding and current row) as last_known_store_sale_revenue
    from scored
    where product_name is not null
),
cleaned as (
    select
        id,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        tags,
        store_sale_revenue,
        store_sale_revenue_percent_rank,
        brand_stddev_samp_store_sale_revenue,
        corr_stock_qty_price,
        next_brand_stddev_samp_store_sale_revenue,
        last_known_store_sale_revenue,
        store_sale_revenue / nullif(corr_stock_qty_price, 0) as store_sale_revenue_per_corr_stock_qty_price
    from metrics
    where launch_date >= to_date('2023-02-28')
      and brand is not null
),
filtered as (
    select
        id,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        store_sale_revenue_percent_rank,
        brand_stddev_samp_store_sale_revenue,
        corr_stock_qty_price,
        next_brand_stddev_samp_store_sale_revenue,
        last_known_store_sale_revenue,
        store_sale_revenue_per_corr_stock_qty_price,
        ceil(store_sale_revenue) as store_sale_revenue_whole
    from cleaned
    where brand is not null
      and brand is not distinct from 'Initech'
)
select
    case when cost >= 790 then 'HIGH' else 'LOW' end as cost_tier,
    category,
    date_trunc('year', launch_date) as year_start,
    avg(corr_stock_qty_price) filter (where brand = 'Globex') as globex_avg_corr_stock_qty_price,
    round(sum(brand_stddev_samp_store_sale_revenue) / nullif(count(case when brand = 'Umbrella' then 1 end), 0), 2) as brand_stddev_samp_store_sale_revenue_per_umbrella,
    count(*) as row_count,
    percentile_approx(store_sale_revenue_per_corr_stock_qty_price, 0.95) as store_sale_revenue_per_corr_stock_qty_price_p95
from filtered
group by grouping sets ((case when cost >= 790 then 'HIGH' else 'LOW' end, category, date_trunc('year', launch_date)), (case when cost >= 790 then 'HIGH' else 'LOW' end, category), (case when cost >= 790 then 'HIGH' else 'LOW' end), ())
having count(*) > 1
order by brand_stddev_samp_store_sale_revenue_per_umbrella;
