WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS dedup_rank
        FROM order_items
    ) t
    WHERE dedup_rank = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity DESC, id) AS dedup_rank_2
        FROM latest
    ) t
    WHERE dedup_rank_2 = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        unit_price,
        dedup_rank_2,
        ROW_NUMBER() OVER (ORDER BY dedup_rank DESC, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY dedup_rank_2 DESC, id) AS row_seq,
        RANK() OVER (PARTITION BY order_id, product_id ORDER BY quantity NULLS LAST) AS quantity_rank,
        LAG(unit_price, 1) OVER w_part AS prev_unit_price
    FROM deduped
    WINDOW w_part AS (ORDER BY dedup_rank, id)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        dedup_rank_2,
        event_seq,
        row_seq,
        quantity_rank,
        prev_unit_price,
        ROUND(dedup_rank_2 * 1.15, 2) AS adjusted_dedup_rank_2,
        unit_price + prev_unit_price AS unit_price_plus_prev_unit_price
    FROM with_windows
    WHERE dedup_rank_2 <= 1
),
latest_order_items AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        dedup_rank_2,
        event_seq,
        unit_price_plus_prev_unit_price
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY prev_unit_price NULLS FIRST, id) = 1
)
-- Grouped metrics
SELECT
    CASE WHEN event_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
    CASE WHEN unit_price_plus_prev_unit_price >= 90 THEN 'LARGE' ELSE 'SMALL' END AS unit_price_plus_prev_unit_price_tier,
    APPROX_PERCENTILE(event_seq, 0.9) AS event_seq_p90,
    APPROX_PERCENTILE(event_seq, 0.5) AS event_seq_p50,
    COUNT(*) AS event_count,
    GROUPING(CASE WHEN event_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
FROM latest_order_items
GROUP BY CUBE (CASE WHEN event_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN unit_price_plus_prev_unit_price >= 90 THEN 'LARGE' ELSE 'SMALL' END)
HAVING APPROX_PERCENTILE(event_seq, 0.9) > 0
ORDER BY event_seq_p90 DESC;
