select ss.sale_date, ss.revenue, ss.product_id, p.brand from store_sales ss inner join products p on p.id = ss.product_id order by ss.sale_date desc
