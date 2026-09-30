select i.id, i.last_restock, p.category, p.stock_qty from inventory i left join products p on p.id = i.product_id where i.product_id in (4, 9) order by 2, p.category;
