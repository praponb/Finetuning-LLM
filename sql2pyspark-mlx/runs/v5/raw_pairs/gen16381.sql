select
    date_format(sale_date, 'yyyy-MM') as sale_month,
    store_id,
    round(avg(units), 2) as avg_units,
    max(sale_date) as latest_sale_date
from store_sales
group by 1, 2
order by store_id desc;
