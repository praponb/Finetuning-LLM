WITH lines AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, attr_value
),
enriched AS (
    SELECT
        id,
        created_at,
        k,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY created_at, id, k) AS seq_no,
        FIRST_VALUE(k) IGNORE NULLS OVER w_part AS first_known_k,
        LAST_VALUE(k) IGNORE NULLS OVER w_part AS last_known_k,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY created_at DESC, id, k) AS seq_no_2,
        LAST_VALUE(k) IGNORE NULLS OVER w_part AS last_known_k_2
    FROM lines
    WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
    WINDOW w_part AS (PARTITION BY k ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        id,
        k,
        seq_no,
        first_known_k,
        last_known_k_2,
        CUME_DIST() OVER (PARTITION BY k ORDER BY seq_no DESC NULLS LAST) AS seq_no_cume_dist,
        COUNT(seq_no_2) OVER (PARTITION BY k ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS seq_no_2_count_1h,
        LAST_VALUE(seq_no_2) IGNORE NULLS OVER (PARTITION BY k ORDER BY seq_no_2, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no_2
    FROM enriched
),
with_windows AS (
    SELECT
        id,
        k,
        last_known_k_2,
        seq_no_cume_dist,
        seq_no_2_count_1h,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY k), 4) AS seq_no_share,
        CUME_DIST() OVER (PARTITION BY k ORDER BY seq_no_2_count_1h DESC) AS seq_no_2_count_1h_cume_dist,
        CUME_DIST() OVER (PARTITION BY k ORDER BY seq_no_cume_dist DESC NULLS FIRST) AS seq_no_cume_dist_cume_dist,
        NTILE(4) OVER w_ordered AS last_known_seq_no_2_bucket
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY k ORDER BY last_known_seq_no_2, id, k)
)
SELECT
    CASE WHEN seq_no_share >= 0.81 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_share_tier,
    k,
    COUNT_IF(k = 'weight_kg') AS weight_kg_events,
    COUNT(DISTINCT k) AS distinct_ks,
    APPROX_PERCENTILE(seq_no_2_count_1h, ARRAY(0.25, 0.5, 0.75)) AS seq_no_2_count_1h_quartiles
FROM with_windows
GROUP BY 1, 2
ORDER BY weight_kg_events NULLS LAST;
