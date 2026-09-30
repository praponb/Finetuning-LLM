WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS code
),
base AS (
    SELECT *, DATE_FORMAT(created_at, 'yyyy-MM') AS created_at_label
    FROM exploded
    WHERE created_at > TO_DATE('2025-03-01')
      AND code IS NOT NULL
),
enriched AS (
    SELECT
        e.*,
        b.email,
        b.name,
        b.country
    FROM base e
    INNER JOIN customers b
        ON e.customer_id = b.id
),
cleaned AS (
    SELECT
        *,
        CASE WHEN code = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle,
        LENGTH(name) AS name_length
    FROM enriched
    WHERE code IS DISTINCT FROM 'WELCOME5'
),
valid AS (
    SELECT *, LOWER(name) AS name_lower
    FROM cleaned
    WHERE created_at >= TO_DATE('2025-03-01')
      AND code IS DISTINCT FROM 'WELCOME5'
),
metrics AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY country, code ORDER BY is_bundle DESC NULLS LAST) AS is_bundle_cume_dist,
        STDDEV_SAMP(is_bundle) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_is_bundle,
        DENSE_RANK() OVER w_grp AS is_bundle_dense_rank,
        AVG(is_bundle) OVER (PARTITION BY code) AS code_avg_is_bundle
    FROM valid
    WHERE name LIKE '%Ross%'
    WINDOW w_grp AS (PARTITION BY code, customer_id ORDER BY is_bundle NULLS FIRST)
)
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    ROUND(SUM(is_bundle_cume_dist) / NULLIF(COUNT(CASE WHEN code = 'BUNDLE' THEN 1 END), 0), 2) AS is_bundle_cume_dist_per_bundle,
    MIN(created_at) AS first_created_at,
    ROUND(SUM(is_bundle_dense_rank) / NULLIF(COUNT(CASE WHEN country = 'SG' THEN 1 END), 0), 2) AS is_bundle_dense_rank_per_sg
FROM metrics
GROUP BY DATE_TRUNC('HOUR', created_at)
ORDER BY is_bundle_dense_rank_per_sg DESC NULLS LAST;
