WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(status) AS status_lower,
        COALESCE(method, 'N/A') AS method_filled
    FROM payments
    WHERE amount <= 630
)
-- Multi-level aggregation
SELECT
    order_id,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(AVG(amount), 2) AS avg_amount_2,
    GROUPING_ID() AS grouping_level
FROM valid
GROUP BY ROLLUP (order_id)
HAVING avg_amount > 0
