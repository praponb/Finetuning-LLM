WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, attr_value
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        service,
        host,
        k,
        CONCAT(k, ', ', service) AS k_service
    FROM exploded
    WHERE host <> 'h3'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(k) AS k_length,
        TRIM(host) AS clean_host
    FROM cleaned
    WHERE host IN ('h3', 'h1', 'h2')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY k, service ORDER BY k_length DESC) AS k_length_cume_dist,
        DENSE_RANK() OVER w_part AS k_length_dense_rank,
        MIN(k_length) OVER w_part_2 AS k_min_k_length,
        STDDEV_SAMP(k_length) OVER (PARTITION BY host, service) AS host_stddev_samp_k_length,
        COUNT(k_length) OVER (PARTITION BY service, k) AS service_count_k_length
    FROM filtered
    WINDOW w_part AS (PARTITION BY k, service ORDER BY k_length DESC NULLS LAST), w_part_2 AS (PARTITION BY k, service)
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN k_length_dense_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END AS k_length_dense_rank_tier,
        AVG(k_length_cume_dist) FILTER (WHERE service = 'search') AS search_avg_k_length_cume_dist,
        COUNT(DISTINCT host) AS distinct_hosts,
        COUNT(*) FILTER (WHERE k = 'version') AS version_rows,
        COUNT_IF(host = 'h1') AS h1_events,
        SUM(CASE WHEN service = 'checkout' THEN k_length_dense_rank ELSE 0 END) AS checkout_k_length_dense_rank,
        COUNT(*) FILTER (WHERE k = 'region') AS region_rows
    FROM with_windows
    GROUP BY CASE WHEN k_length_dense_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END
    HAVING region_rows > 1
)
SELECT
    k_length_dense_rank_tier,
    distinct_hosts,
    version_rows,
    h1_events,
    checkout_k_length_dense_rank
FROM daily;
