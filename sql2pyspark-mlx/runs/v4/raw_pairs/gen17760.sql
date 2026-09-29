SELECT id, unit_price, ROW_NUMBER() OVER (ORDER BY unit_price DESC, id) rn
FROM order_items;
