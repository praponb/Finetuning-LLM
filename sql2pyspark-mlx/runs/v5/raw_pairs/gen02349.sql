select
    id,
    sale_date,
    store_id,
    units + revenue as revenue_per_unit,
    round(units / revenue, 2) as revenue_per_unit_2
from store_sales
where (revenue <= 2700 and month(sale_date) in (1, 6, 11)) or store_id in (1, 3)
