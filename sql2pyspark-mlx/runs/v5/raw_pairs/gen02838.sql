select ss.id, ss.sale_date, ss.units from store_sales ss where ss.product_id in (5, 6, 7, 8) and ss.product_id in (select id from products where cost <= 450) order by ss.units desc;
