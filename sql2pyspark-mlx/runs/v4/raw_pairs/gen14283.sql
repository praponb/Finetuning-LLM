SELECT
    CASE WHEN quantity >= 242 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    AVG(quantity) AS avg_quantity,
    BIT_AND(quantity) AS quantity_bit_and,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    BIT_OR(quantity) AS quantity_bit_or
FROM inventory
GROUP BY 1
HAVING quantity_bit_and < 1
ORDER BY quantity_bit_or
