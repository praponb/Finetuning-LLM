WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        ROUND(unit_price * 0.9, 2) AS adjusted_unit_price,
        CASE WHEN unit_price >= 320 THEN 'High' WHEN unit_price >= 80 THEN 'Medium' ELSE 'Low' END AS unit_price_band
    FROM order_items
    WHERE unit_price <= 290
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        unit_price,
        adjusted_unit_price,
        COUNT(unit_price) OVER (PARTITION BY product_id ORDER BY adjusted_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_unit_price,
        AVG(unit_price) OVER (PARTITION BY product_id ORDER BY adjusted_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_unit_price,
        SUM(unit_price) OVER (PARTITION BY product_id ORDER BY adjusted_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_unit_price,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS event_seq
    FROM valid
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY running_count_unit_price DESC NULLS LAST, id) = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY running_avg_unit_price DESC, id) AS latest_rank
        FROM deduped
    ) t
    WHERE latest_rank = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        product_id,
        running_sum_unit_price,
        latest_rank
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY event_seq DESC) = 1
)
-- Running and moving aggregates
SELECT
    id,
    product_id,
    RANK() OVER (PARTITION BY product_id ORDER BY running_sum_unit_price) AS running_sum_unit_price_rank,
    LEAD(running_sum_unit_price, 1, 0) OVER (ORDER BY running_sum_unit_price, id) AS next_running_sum_unit_price,
    CUME_DIST() OVER (ORDER BY running_sum_unit_price) AS running_sum_unit_price_cume_dist,
    AVG(running_sum_unit_price) OVER (ORDER BY latest_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS running_sum_unit_price_avg_6
FROM latest
