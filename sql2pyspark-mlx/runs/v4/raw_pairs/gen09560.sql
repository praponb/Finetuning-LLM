select store_id, date_trunc('MONTH', sale_date) sale_month, min(revenue) min_revenue from store_sales group by store_id, sale_month having min(revenue) >= 1600 order by store_id, sale_month limit 3;
