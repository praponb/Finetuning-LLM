WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS code
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(code, 1, 4) AS code_prefix,
        COALESCE(code, 'N/A') AS code_filled
    FROM flattened
    WHERE created_at < '2023-06-03'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        created_at,
        code,
        LENGTH(code) AS code_length,
        DATE_TRUNC('WEEK', created_at) AS created_at_period
    FROM filtered
    WHERE created_at >= TO_DATE('2025-03-02')
      AND code IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(code_length) OVER (PARTITION BY code) AS code_avg_code_length,
        MAX(code_length) OVER (PARTITION BY code ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS code_length_max_15m,
        DENSE_RANK() OVER (PARTITION BY code ORDER BY code_length DESC NULLS LAST) AS code_length_dense_rank,
        MIN(code_length) OVER (PARTITION BY code) AS code_min_code_length
    FROM base
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY code ORDER BY code_length_max_15m DESC NULLS LAST) AS code_length_max_15m_rank,
        COUNT(code_length) OVER w_recent AS code_length_count_30m
    FROM scored
    WINDOW w_recent AS (PARTITION BY code ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
)
-- Window metrics over several different partitions
SELECT
    *,
    ROUND(code_length_max_15m_rank / SUM(code_length_max_15m_rank) OVER (PARTITION BY code), 4) AS code_length_max_15m_rank_share,
    PERCENT_RANK() OVER (ORDER BY code_length_count_30m DESC NULLS FIRST) AS code_length_count_30m_percent_rank
FROM with_windows;
