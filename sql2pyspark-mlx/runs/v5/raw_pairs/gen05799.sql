WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        ends_on,
        CEIL(discount_pct) AS discount_pct_whole,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM promotions
    WHERE ends_on > DATE '2023-09-29'
      AND channel IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        channel,
        discount_pct_whole,
        resolved_channel
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on DESC, id) AS latest_rank
        FROM base
    ) x
    WHERE latest_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        LEAD(discount_pct_whole, 2, 0) OVER (PARTITION BY channel ORDER BY discount_pct_whole, id) AS next_discount_pct_whole,
        ROW_NUMBER() OVER w_running AS row_seq,
        NTILE(4) OVER (ORDER BY discount_pct_whole, id) AS discount_pct_whole_bucket
    FROM ranked
    WINDOW w_running AS (PARTITION BY channel ORDER BY discount_pct_whole DESC, id)
),
ranked_promotions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct_whole_bucket, id) AS latest_rank
        FROM metrics
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    id,
    channel,
    next_discount_pct_whole,
    row_seq,
    discount_pct_whole_bucket,
    CASE WHEN channel RLIKE '^(partner|web)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS channel_zone
FROM ranked_promotions
