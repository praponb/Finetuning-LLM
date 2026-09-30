select
    id,
    store_id,
    units,
    round(revenue / units, 2) as revenue_per_unit
from store_sales
where store_id in (1, 3, 4, 5) and sale_date > '2025-05-01' and store_id in (1, 2, 4)
