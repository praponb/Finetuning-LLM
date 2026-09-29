select ss.id, ss.product_id, ss.revenue from store_sales ss where ss.units < 28 and ss.product_id in (select id from products where category = 'Electronics') order by revenue desc;
