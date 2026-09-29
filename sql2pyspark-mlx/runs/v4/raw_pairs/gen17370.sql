select ss.id, ss.sale_date, ss.revenue, p.launch_date, p.stock_qty, p.cost from store_sales ss inner join products p on ss.product_id = p.id order by id desc;
