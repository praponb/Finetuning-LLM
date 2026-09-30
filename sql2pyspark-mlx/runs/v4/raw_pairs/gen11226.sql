select
    date_trunc('quarter', launch_date) as quarter_start,
    case when stock_qty >= 448 then 'PREMIUM' else 'STANDARD' end as stock_qty_tier,
    brand,
    sum(stock_qty) filter (where brand = 'Stark') as stark_total_stock_qty,
    stddev_samp(cost) as cost_stddev,
    count(*) filter (where brand = 'Acme') as acme_rows,
    count(distinct brand) as distinct_brands,
    grouping_id() as grouping_level
from products
group by rollup (date_trunc('quarter', launch_date), case when stock_qty >= 448 then 'PREMIUM' else 'STANDARD' end, brand)
having distinct_brands > 1
order by quarter_start desc nulls last;
