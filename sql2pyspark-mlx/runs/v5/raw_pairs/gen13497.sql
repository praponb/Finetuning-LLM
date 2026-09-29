WITH flattened AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS attr_key, attr_value
),
cleaned AS (
    SELECT *, SORT_ARRAY(coupon_codes) AS sorted_coupon_codes
    FROM flattened
    WHERE created_at > DATE '2025-03-01'
      AND attr_key IS DISTINCT FROM 'device'
),
filtered AS (
    SELECT
        *,
        SIZE(coupon_codes) AS coupon_codes_count,
        DATE_FORMAT(created_at, 'MMM yyyy') AS created_at_label
    FROM cleaned
    WHERE created_at > TIMESTAMP '2025-03-02 03:00:00'
      AND attr_key IS NOT NULL
),
enriched AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY attr_key ORDER BY coupon_codes_count NULLS FIRST) AS coupon_codes_count_rank,
        SUM(coupon_codes_count) OVER (PARTITION BY attr_key ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS coupon_codes_count_sum_1h
    FROM filtered
)
SELECT
    attr_key,
    CASE WHEN coupon_codes_count_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END AS coupon_codes_count_rank_tier,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(coupon_codes_count_sum_1h, 0.95) AS coupon_codes_count_sum_1h_p95,
    ROUND(AVG(coupon_codes_count_rank), 2) AS avg_coupon_codes_count_rank,
    GROUPING(attr_key) AS gid
FROM enriched
GROUP BY GROUPING SETS ((attr_key, CASE WHEN coupon_codes_count_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END), (attr_key))
