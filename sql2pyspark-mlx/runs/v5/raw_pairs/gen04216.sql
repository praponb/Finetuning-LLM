select
    id,
    product_id,
    sale_date,
    units,
    last_value(units) over (partition by product_id order by sale_date desc, id rows between unbounded preceding and unbounded following) as last_units
from store_sales;
