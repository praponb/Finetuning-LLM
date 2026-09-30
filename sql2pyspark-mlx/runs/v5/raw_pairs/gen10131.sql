select p.id, p.cost, p.price, p.category from products p where exists (select 1 from inventory i where i.product_id = p.id and year(i.last_restock) = 2023);
