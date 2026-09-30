WITH lines AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, value
),
enriched AS (
    SELECT
        s.id,
        s.customer_id,
        s.k,
        b.age,
        b.segment,
        b.country
    FROM lines s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
metrics AS (
    SELECT
        *,
        LEAD(age, 1) OVER (PARTITION BY country, k ORDER BY age, id, k) AS next_age,
        AVG(age) OVER (PARTITION BY k ORDER BY age, id, k ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS age_avg_6,
        CUME_DIST() OVER (ORDER BY age DESC) AS age_cume_dist
    FROM enriched
    WHERE country = 'DE'
),
with_dims AS (
    SELECT
        s.id,
        s.customer_id,
        s.k,
        s.age,
        s.segment,
        s.country,
        s.next_age,
        s.age_cume_dist,
        b.age AS customer_age,
        b.name
    FROM metrics s
    LEFT JOIN customers b
        ON s.customer_id <=> b.id
),
base AS (
    SELECT
        *,
        ROUND(age_cume_dist, 2) AS age_cume_dist_rounded,
        LEAST(age, age_cume_dist) AS least_age_age_cume_dist,
        COALESCE(k, 'UNKNOWN') AS k_filled
    FROM with_dims
    WHERE customer_id IS NOT NULL
),
enriched_carts AS (
    SELECT
        *,
        RANK() OVER w_grp AS age_rank,
        NTILE(5) OVER (PARTITION BY k, segment ORDER BY next_age, id, k) AS next_age_bucket,
        NTILE(3) OVER (PARTITION BY k ORDER BY least_age_age_cume_dist, id, k) AS least_age_age_cume_dist_bucket,
        AVG(next_age) OVER (ORDER BY customer_age, id, k ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS next_age_avg_2_prior,
        DENSE_RANK() OVER w_ordered AS age_cume_dist_dense_rank
    FROM base
    WINDOW w_grp AS (ORDER BY age NULLS LAST), w_ordered AS (PARTITION BY customer_id ORDER BY age_cume_dist DESC NULLS LAST)
)
SELECT
    d.country AS customer_country,
    s.segment AS cart_segment,
    COUNT(*) AS num_rows,
    MIN(s.next_age_avg_2_prior) AS min_next_age_avg_2_prior,
    MIN(s.age_cume_dist_rounded) AS min_age_cume_dist_rounded
FROM enriched_carts s
LEFT JOIN customers d ON s.customer_id = d.id
GROUP BY ROLLUP (d.country, s.segment)
