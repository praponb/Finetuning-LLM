select revenue, sale_date, cast(revenue as int) as revenue_int from store_sales where units >= 38 or sale_date <= '2024-02-10';
