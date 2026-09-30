WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, v
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        host,
        k,
        CASE WHEN service = 'checkout' THEN 1 ELSE 0 END AS is_checkout,
        LOWER(host) AS host_lower,
        COALESCE(k, 'NONE') AS k_filled
    FROM flattened
    WHERE service = 'auth'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        host,
        k,
        is_checkout,
        k_filled,
        CASE WHEN is_checkout >= 76 THEN 'High' WHEN is_checkout >= 33 THEN 'Medium' ELSE 'Low' END AS is_checkout_tier,
        CASE WHEN k IN ('trace', 'version') THEN 'Group A' ELSE 'Other' END AS k_group
    FROM cleaned
    WHERE k IS NOT DISTINCT FROM 'version'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(is_checkout AS DOUBLE) AS is_checkout_str,
        UPPER(host) AS host_upper,
        COALESCE(k, 'N/A') AS k_clean
    FROM valid
    WHERE k IS DISTINCT FROM 'trace'
      AND host != 'h1'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY is_checkout_str DESC NULLS FIRST) AS is_checkout_str_dense_rank,
        RANK() OVER (ORDER BY is_checkout_str DESC NULLS LAST) AS is_checkout_str_rank,
        SUM(is_checkout) OVER (PARTITION BY host) AS host_sum_is_checkout
    FROM base
),
valid_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        host,
        k,
        k_filled,
        is_checkout_tier,
        k_group,
        is_checkout_str,
        is_checkout_str_dense_rank,
        is_checkout_str_rank,
        host_sum_is_checkout,
        LEAST(host_sum_is_checkout, is_checkout_str) AS least_host_sum_is_checkout_is_checkout_str,
        ROUND(is_checkout_str_dense_rank, 1) AS is_checkout_str_dense_rank_rounded,
        COALESCE(k, 'N/A') AS k_filled_2
    FROM scored
    WHERE host = 'h4'
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, DENSE_RANK() OVER (ORDER BY is_checkout_str_rank DESC NULLS FIRST) AS is_checkout_str_rank_dense_rank
    FROM valid_app_logs
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN least_host_sum_is_checkout_is_checkout_str >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS least_host_sum_is_checkout_is_checkout_str_tier,
    COUNT(DISTINCT k) AS distinct_ks,
    PERCENTILE_APPROX(is_checkout_str_dense_rank, 0.95) AS is_checkout_str_dense_rank_p95
FROM with_windows
GROUP BY CASE WHEN least_host_sum_is_checkout_is_checkout_str >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY least_host_sum_is_checkout_is_checkout_str_tier ASC;
