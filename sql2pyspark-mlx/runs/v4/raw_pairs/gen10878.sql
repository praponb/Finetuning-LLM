WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        pos,
        line.product_id,
        line.qty,
        line.promo
    FROM carts
    LATERAL VIEW POSEXPLODE(items) e AS pos, line
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.segment,
        d.country,
        d.city
    FROM lines s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY product_id NULLS FIRST, id, pos) AS rn
    FROM joined
    QUALIFY rn = 1
)
-- Grouped metrics
SELECT
    customer_id,
    product_id,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    COUNT(*) AS cnt,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list_2,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) AS cnt_dense_rank
FROM ranked
GROUP BY customer_id, product_id
ORDER BY cnt DESC;
