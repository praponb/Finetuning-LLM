SELECT *, COALESCE(unit_price, 0) unit_price_filled FROM order_items;
