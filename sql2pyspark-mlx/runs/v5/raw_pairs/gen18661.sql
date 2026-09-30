WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_TRUNC('year', opened_date) AS opened_date_period,
        COALESCE(region, 'OTHER') AS region_filled
    FROM stores
    WHERE region NOT IN ('East', 'North')
      AND region IS NOT DISTINCT FROM 'North'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        sqft,
        opened_date_period,
        region_filled,
        NTILE(4) OVER w_grp AS sqft_bucket,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        AVG(sqft) OVER (PARTITION BY region, city) AS region_avg_sqft
    FROM valid
    WINDOW w_grp AS (ORDER BY sqft DESC, id), w_ordered AS (ORDER BY sqft, id)
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(opened_date_period) OVER (PARTITION BY city ORDER BY opened_date_period, id) AS prev_opened_date_period
    FROM scored
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_opened_date_period IS NULL OR opened_date_period > prev_opened_date_period + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_stores AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY city ORDER BY opened_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY region_avg_sqft DESC, id) AS latest_rank
    FROM sessions_stores
    QUALIFY latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    city,
    island_id,
    COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count,
    SUM(CASE WHEN city = 'London' THEN sqft_bucket ELSE 0 END) AS london_sqft_bucket,
    SUM(row_seq) AS total_row_seq
FROM latest
GROUP BY city, island_id
HAVING SUM(CASE WHEN city = 'London' THEN sqft_bucket ELSE 0 END) < 1000000
ORDER BY total_row_seq;
