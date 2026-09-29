select
    category,
    quarter(launch_date) launch_quarter,
    round(stddev(stock_qty), 2) stddev_stock_qty,
    round((100 * sum(case when brand = 'Acme' then 1 else 0 end)) / count(*), 1) pct_acme
from products
group by category, quarter(launch_date);
