select ss.id, ss.units, p.stock_qty, p.product_name from store_sales ss join products p on p.id = ss.product_id order by ss.units desc;
