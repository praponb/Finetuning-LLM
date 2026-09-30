with filtered_store_sales as (
    select *, coalesce(units, 0) as units_filled
    from store_sales
    where revenue is not null and sale_date < '2025-01-24'
),
store_sales_stats as (
    select date_format(sale_date, 'yyyy-MM') as sale_month, year(sale_date) as sale_year, count(revenue) as revenue_count
    from filtered_store_sales
    group by date_format(sale_date, 'yyyy-MM'), year(sale_date)
)
select sale_month, revenue_count
from store_sales_stats
order by revenue_count desc, sale_month;
