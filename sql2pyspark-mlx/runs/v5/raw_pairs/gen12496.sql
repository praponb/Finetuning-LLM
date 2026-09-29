WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM order_items
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        unit_price,
        SUM(unit_price) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        SUM(unit_price) OVER w_recent AS cumulative_unit_price
    FROM ranked
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT
    SELECT id
    FROM order_items
    WHERE unit_price <= 160
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        unit_price,
        running_unit_price,
        cumulative_unit_price,
        ROUND(cumulative_unit_price / SUM(cumulative_unit_price) OVER (PARTITION BY order_id), 4) AS cumulative_unit_price_share,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY cumulative_unit_price DESC NULLS LAST) AS cumulative_unit_price_percent_rank,
        CUME_DIST() OVER (ORDER BY running_unit_price DESC NULLS FIRST) AS running_unit_price_cume_dist
    FROM verified
    WHERE cumulative_unit_price < 892
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN unit_price >= 320 THEN 'HIGH' ELSE 'LOW' END AS unit_price_tier,
    CASE WHEN cumulative_unit_price_percent_rank >= 0.65 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_unit_price_percent_rank_tier,
    PERCENTILE_APPROX(cumulative_unit_price_share, 0.95) AS cumulative_unit_price_share_p95,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(cumulative_unit_price_share, ARRAY(0.25, 0.5, 0.75)) AS cumulative_unit_price_share_quartiles
FROM scored
GROUP BY 1, 2
