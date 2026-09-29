select oi.id, oi.quantity, oi.product_id, o.store_id, p.cost, p.stock_qty from order_items oi inner join orders o on oi.order_id = o.id left join products p on oi.product_id = p.id
