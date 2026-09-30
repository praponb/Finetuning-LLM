select ss.sale_date, p.product_name, p.stock_qty, p.supplier_id from store_sales ss inner join products p on ss.product_id = p.id
