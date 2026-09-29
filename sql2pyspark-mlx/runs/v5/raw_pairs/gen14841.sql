select ss.revenue, ss.units, ss.store_id, p.category, p.price, p.cost from store_sales ss inner join products p on ss.product_id = p.id
