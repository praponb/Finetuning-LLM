select id, product_id, unit_price, min(unit_price) over (partition by product_id) as product_id_min_unit_price, ntile(2) over (order by unit_price desc, id) as bucket from order_items;
