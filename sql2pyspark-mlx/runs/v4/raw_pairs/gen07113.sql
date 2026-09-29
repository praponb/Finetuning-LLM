select sale_date, revenue, revenue + units as revenue_per_unit from store_sales where not (product_id in (6, 8, 10) or product_id in (3, 4, 6, 7)) order by revenue desc, 3 desc
