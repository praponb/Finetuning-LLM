SELECT
    CASE WHEN quantity >= 9 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    CORR(unit_price, quantity) AS corr_unit_price_quantity,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    BIT_AND(quantity) AS quantity_bit_and,
    ROUND(AVG(unit_price), 2) AS avg_unit_price_2,
    COUNT(DISTINCT order_id) AS distinct_order_ids
FROM order_items
GROUP BY CASE WHEN quantity >= 9 THEN 'LARGE' ELSE 'SMALL' END
HAVING ROUND(AVG(unit_price), 2) < 0;
