select
    store_id,
    revenue,
    sale_date,
    coalesce(revenue, 0) as revenue_filled,
    date_sub(sale_date, 11) as sale_date_minus_11d
from store_sales
where units != 27 and revenue > 3600;
