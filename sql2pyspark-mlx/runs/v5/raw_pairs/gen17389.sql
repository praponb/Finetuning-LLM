WITH base AS (
    SELECT
        *,
        DATE_TRUNC('month', ends_on) ends_on_period,
        CASE WHEN discount_pct >= 0.31 THEN 'High' WHEN discount_pct >= 0.1 THEN 'Medium' ELSE 'Low' END discount_pct_level
    FROM promotions
    WHERE ends_on > DATE '2023-08-03'
      AND promo_code NOT IN ('BUNDLE', 'SPRING10', 'WELCOME5')
),
ranked AS (
    SELECT id, ends_on
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on DESC, id) = 1
),
with_windows AS (
    SELECT
        id,
        ends_on,
        ROW_NUMBER() OVER (ORDER BY ends_on DESC, id) row_seq,
        ROW_NUMBER() OVER (ORDER BY ends_on, id) event_seq,
        ROW_NUMBER() OVER (ORDER BY ends_on DESC, id) seq_no,
        ROW_NUMBER() OVER (ORDER BY ends_on, id) event_seq_2
    FROM ranked
),
base_promotions AS (
    SELECT
        *,
        LEAST(seq_no, row_seq) least_seq_no_row_seq,
        CAST(event_seq AS DOUBLE) event_seq_num
    FROM with_windows
    WHERE ends_on >= DATE '2023-10-13'
)
SELECT
    DATE_TRUNC('WEEK', ends_on) AS week_start,
    DATE_TRUNC('QUARTER', ends_on) AS quarter_start,
    seq_no,
    ROUND(AVG(event_seq_num), 2) AS avg_event_seq_num,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(least_seq_no_row_seq), 2) AS avg_least_seq_no_row_seq,
    ROUND(AVG(event_seq_num), 2) AS avg_event_seq_num_2
FROM base_promotions
GROUP BY 1, 2, 3;
