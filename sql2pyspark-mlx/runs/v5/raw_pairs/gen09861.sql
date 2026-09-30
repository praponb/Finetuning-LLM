select max(sale_date) as latest_sale_date, round(avg(revenue), 2) as avg_revenue, max(sale_date) as last_sale_date from store_sales where units > 35 order by avg_revenue
