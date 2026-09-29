with valid as (
    select *, substring(brand, 1, 3) as brand_prefix
    from products
    where supplier_id is not null
      and price >= 1050
)
select
    date_trunc('month', launch_date) as activity_month,
    supplier_id,
    approx_percentile(cost, 0.5) as cost_p50,
    stddev_samp(cost) as cost_stddev,
    count(case when category = 'Books' then 1 end) as books_count,
    percentile_approx(cost, 0.95) as cost_p95,
    percentile_approx(price, array(0.25, 0.5, 0.75)) as price_quartiles
from valid
group by date_trunc('month', launch_date), supplier_id
order by cost_p50 asc;
