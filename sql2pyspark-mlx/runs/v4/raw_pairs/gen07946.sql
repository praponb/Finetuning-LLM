select products.id, products.stock_qty from products where not exists (select 1 from order_items where order_items.product_id = products.id and order_items.unit_price between 270 and 370)
