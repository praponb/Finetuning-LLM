select *, coalesce(units, 0) as units_filled, add_months(sale_date, 3) as sale_date_plus_months
from store_sales
order by sale_date_plus_months
