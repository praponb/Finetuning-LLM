WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        last_restock,
        DATE_TRUNC('week', last_restock) AS last_restock_period
    FROM inventory
    WHERE quantity = 105
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last_restock_period,
        ROW_NUMBER() OVER w_recent AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS event_seq_2,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_period DESC, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY last_restock DESC, id) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY last_restock, id) AS row_seq_2
    FROM cleaned
    WHERE product_id IN (1, 4, 9)
    WINDOW w_recent AS (ORDER BY last_restock_period, id)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        last_restock_period,
        event_seq,
        event_seq_2,
        ROUND(event_seq_2 * 1.15, 2) AS adjusted_event_seq_2
    FROM enriched
    WHERE row_seq <= 5
),
enriched_inventory AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_seq_2,
        adjusted_event_seq_2,
        LAG(event_seq_2, 2, 0) OVER (ORDER BY event_seq_2, id) AS prev_event_seq_2,
        ROW_NUMBER() OVER (ORDER BY event_seq_2, id) AS row_seq
    FROM filtered
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN adjusted_event_seq_2 >= 60 THEN 'High' WHEN adjusted_event_seq_2 >= 33 THEN 'Medium' ELSE 'Low' END AS adjusted_event_seq_2_tier,
        CASE WHEN prev_event_seq_2 >= 7 THEN 'High' WHEN prev_event_seq_2 >= 4 THEN 'Medium' ELSE 'Low' END AS prev_event_seq_2_level
    FROM enriched_inventory
    WHERE row_seq <= 3
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        adjusted_event_seq_2,
        adjusted_event_seq_2_tier,
        prev_event_seq_2_level,
        LEAD(row_seq) OVER w_recent AS next_row_seq,
        LAG(row_seq) OVER w_recent AS prev_row_seq,
        LAG(event_seq_2) OVER w_recent AS prev_event_seq_2_2,
        LEAD(event_seq_2) OVER w_recent AS next_event_seq_2,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM base
    WINDOW w_recent AS (ORDER BY event_seq_2, id)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        adjusted_event_seq_2_tier,
        prev_event_seq_2_level,
        next_row_seq,
        prev_row_seq,
        prev_event_seq_2_2,
        ROUND(next_row_seq, 0) AS next_row_seq_rounded
    FROM metrics
    WHERE prev_row_seq > 3
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN prev_event_seq_2_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_event_seq_2_2_tier,
        CASE WHEN next_row_seq >= 8 THEN 'HIGH' ELSE 'LOW' END AS next_row_seq_tier,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(prev_row_seq), 2) AS avg_prev_row_seq,
        COUNT(*) AS cnt_2
    FROM valid
    GROUP BY CASE WHEN prev_event_seq_2_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN next_row_seq >= 8 THEN 'HIGH' ELSE 'LOW' END
    HAVING ROUND(AVG(prev_row_seq), 2) < 1000000
)
SELECT
    prev_event_seq_2_2_tier,
    next_row_seq_tier,
    cnt_2
FROM totals;
