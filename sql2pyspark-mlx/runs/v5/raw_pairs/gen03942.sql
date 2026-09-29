SELECT
    CASE WHEN unit_price >= 390 THEN 'PREMIUM' ELSE 'STANDARD' END AS unit_price_tier,
    STDDEV_SAMP(unit_price) AS unit_price_stddev,
    CORR(unit_price, quantity) AS corr_unit_price_quantity
FROM order_items
GROUP BY CASE WHEN unit_price >= 390 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY unit_price_tier DESC NULLS LAST;
