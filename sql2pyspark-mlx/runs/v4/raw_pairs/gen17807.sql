select sale_year, avg_revenue, avg_units
from (
    select year(sale_date) as sale_year, avg(revenue) as avg_revenue, round(avg(units), 2) as avg_units
    from store_sales
    where year(sale_date) >= 2023
    group by year(sale_date)
) as s
where avg_units < 26
order by avg_revenue;
