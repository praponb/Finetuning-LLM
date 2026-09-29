SELECT
    CASE WHEN quantity >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids,
    BIT_AND(quantity) AS quantity_bit_and,
    BIT_AND(quantity) AS quantity_bit_and_2
FROM order_items
GROUP BY CASE WHEN quantity >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END;
