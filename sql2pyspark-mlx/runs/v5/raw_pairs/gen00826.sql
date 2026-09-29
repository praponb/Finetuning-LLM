WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        EXPLODE(coupon_codes) AS code
    FROM carts
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DAYOFWEEK(created_at) AS created_at_dayofweek,
        TO_DATE(created_at) AS created_at_date,
        COALESCE(code, 'UNKNOWN') AS code_clean
    FROM lines
    WHERE created_at >= DATE '2025-03-02'
      AND code IN ('VIP', 'SPRING10', 'BUNDLE')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        created_at,
        MAX(created_at_dayofweek) OVER (PARTITION BY code ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS created_at_dayofweek_max_15m,
        AVG(created_at_dayofweek) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS created_at_dayofweek_avg_30m,
        RANK() OVER (PARTITION BY code ORDER BY created_at_dayofweek DESC NULLS LAST) AS created_at_dayofweek_rank,
        PERCENT_RANK() OVER (PARTITION BY code ORDER BY created_at_dayofweek DESC) AS created_at_dayofweek_percent_rank
    FROM valid
),
valid_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN created_at_dayofweek_percent_rank >= 0.65 THEN 'High' WHEN created_at_dayofweek_percent_rank >= 0.39 THEN 'Medium' ELSE 'Low' END AS created_at_dayofweek_percent_rank_band
    FROM with_windows
    WHERE created_at_dayofweek_avg_30m IS NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, created_at_dayofweek_max_15m - created_at_dayofweek_percent_rank AS created_at_dayofweek_max_15m_minus_created_at_dayofweek_percent_rank
    FROM valid_carts
    WHERE created_at >= '2025-10-01'
)
SELECT
    id,
    created_at_dayofweek_max_15m,
    created_at_dayofweek_avg_30m,
    created_at_dayofweek_rank,
    created_at_dayofweek_percent_rank,
    created_at_dayofweek_percent_rank_band
FROM base
ORDER BY id ASC NULLS FIRST
