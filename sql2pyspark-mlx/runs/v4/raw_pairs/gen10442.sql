WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, v
),
valid AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        v,
        LOWER(attr_key) AS attr_key_lower,
        LOWER(attr_key) AS attr_key_lower_2
    FROM lines
    WHERE created_at >= DATE '2025-03-01'
      AND customer_id IS NOT NULL
),
enriched AS (
    SELECT src.*, c.age
    FROM valid src
    LEFT JOIN customers c
        ON src.customer_id = c.id
)
SELECT
    *,
    AVG(age) OVER w_running AS customer_id_avg_age,
    COUNT(age) OVER w_running AS customer_id_count_age,
    CUME_DIST() OVER w_hist AS age_cume_dist,
    PERCENT_RANK() OVER (ORDER BY age NULLS LAST) AS age_percent_rank
FROM enriched
WINDOW w_running AS (PARTITION BY customer_id), w_hist AS (PARTITION BY attr_key ORDER BY age DESC NULLS LAST);
