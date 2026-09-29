WITH remaining_keys AS (
    SELECT id
    FROM store_sales
    EXCEPT
    SELECT id
    FROM store_sales
    WHERE product_id IN (1, 4)
),
kept_rows AS (
    SELECT s.*
    FROM store_sales s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    CASE WHEN revenue >= 4200 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_tier,
    store_id,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS event_count,
    BIT_AND(units) AS units_bit_and,
    COUNT(*) AS row_count
FROM kept_rows
GROUP BY CASE WHEN revenue >= 4200 THEN 'PREMIUM' ELSE 'STANDARD' END, store_id
ORDER BY units_bit_and DESC NULLS LAST
