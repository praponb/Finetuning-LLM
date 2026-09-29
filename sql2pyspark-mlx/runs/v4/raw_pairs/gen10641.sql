with latest_match as (
    select s.*, x.store_sale_sale_date
    from products s
    left join lateral (
            select c.sale_date as store_sale_sale_date
            from store_sales c
            where c.product_id = s.id
            order by c.units desc, c.id desc
            limit 1
        ) x on true
)
select
    case when price >= 1140 then 'HIGH' else 'LOW' end as price_tier,
    date_trunc('quarter', launch_date) as quarter_start,
    round(avg(cost), 2) as avg_cost,
    percentile_approx(stock_qty, 0.9) as stock_qty_p90,
    count_if(category = 'Grocery') as grocery_events,
    approx_percentile(cost, array(0.25, 0.5, 0.75)) as cost_quartiles,
    size(collect_set(brand)) as num_brands
from latest_match
group by cube (case when price >= 1140 then 'HIGH' else 'LOW' end, date_trunc('quarter', launch_date))
order by quarter_start desc;
