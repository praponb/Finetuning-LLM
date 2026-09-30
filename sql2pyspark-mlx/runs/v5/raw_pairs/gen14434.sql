SELECT
    order_id,
    quantity,
    unit_price,
    CAST(quantity AS DOUBLE) quantity_str,
    CEIL(unit_price) unit_price_whole
FROM order_items
ORDER BY quantity_str ASC, unit_price_whole DESC;
