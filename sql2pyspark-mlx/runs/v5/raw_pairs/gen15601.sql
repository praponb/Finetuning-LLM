WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        DATE_TRUNC('MONTH', ends_on) AS ends_on_period,
        DATEDIFF(CURRENT_DATE(), ends_on) AS days_since_ends_on
    FROM promotions
    WHERE channel IS NOT NULL
      AND ends_on >= TO_DATE('2023-08-18')
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN ends_on_period - LAG(ends_on_period) OVER (PARTITION BY channel ORDER BY ends_on_period, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY channel ORDER BY ends_on_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        discount_pct,
        days_since_ends_on,
        session_no,
        discount_pct - days_since_ends_on AS discount_pct_minus_days_since_ends_on
    FROM islands
    WHERE channel IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY session_no DESC, id) = 1
)
SELECT
    id,
    session_no,
    discount_pct_minus_days_since_ends_on
FROM latest;
