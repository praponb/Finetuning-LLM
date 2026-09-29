select
    store_id,
    product_id,
    units,
    coalesce(units, 0) as units_filled,
    case when units >= 36 then 'High' when units >= 12 then 'Medium' else 'Low' end as units_band
from store_sales
where (revenue >= 3400 and revenue >= 1700) or sale_date <= '2023-09-01'
order by units_band desc
