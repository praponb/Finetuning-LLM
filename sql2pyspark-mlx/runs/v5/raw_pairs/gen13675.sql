select p.brand, ss.revenue, ss.units, ss.product_id from products p left join store_sales ss on p.id = ss.product_id;
