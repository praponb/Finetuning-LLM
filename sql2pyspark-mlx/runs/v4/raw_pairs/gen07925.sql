SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price NULLS FIRST, id) AS rank_in_group
FROM order_items
QUALIFY rank_in_group = 1;
