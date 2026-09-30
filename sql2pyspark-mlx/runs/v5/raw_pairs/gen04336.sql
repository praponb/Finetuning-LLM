WITH exploded AS (
    SELECT
        id,
        customer_id,
        line.qty,
        line.price
    FROM carts
    LATERAL VIEW EXPLODE(items) li AS line
),
joined AS (
    SELECT
        a.*,
        d.age,
        d.segment
    FROM exploded a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
matched_keys AS (
    SELECT customer_id
    FROM joined
    INTERSECT
    SELECT customer_id
    FROM carts
    WHERE created_at < '2025-06-07'
),
verified AS (
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id
),
scored AS (
    SELECT
        id,
        customer_id,
        qty,
        price,
        age,
        segment,
        ROUND(qty / SUM(qty) OVER (PARTITION BY segment), 4) qty_share,
        ROUND(qty / SUM(qty) OVER (PARTITION BY customer_id), 4) qty_share_2,
        ROUND(qty / SUM(qty) OVER (PARTITION BY segment, customer_id), 4) qty_share_3
    FROM verified
),
with_windows AS (
    SELECT
        id,
        price,
        age,
        segment,
        qty_share,
        qty_share_2,
        qty_share_3,
        STDDEV_SAMP(qty_share_2) OVER (PARTITION BY customer_id) customer_id_stddev_samp_qty_share_2,
        PERCENT_RANK() OVER (PARTITION BY customer_id, segment ORDER BY qty_share NULLS LAST) qty_share_percent_rank,
        RANK() OVER (PARTITION BY customer_id ORDER BY qty_share_2 DESC) qty_share_2_rank,
        MAX(age) OVER (PARTITION BY customer_id) customer_id_max_age,
        ROUND(qty / SUM(qty) OVER (PARTITION BY customer_id), 4) qty_share_4
    FROM scored
),
cleaned AS (
    SELECT
        id,
        price,
        age,
        segment,
        qty_share,
        customer_id_stddev_samp_qty_share_2,
        customer_id_max_age,
        qty_share_4,
        ROUND(qty_share_4 * 1.07, 2) qty_share_4_adjusted,
        CASE WHEN customer_id_max_age >= 65 THEN 'High' WHEN customer_id_max_age >= 26 THEN 'Medium' ELSE 'Low' END customer_id_max_age_tier,
        COALESCE(segment, 'OTHER') resolved_segment
    FROM with_windows
    WHERE qty_share_4 BETWEEN 0.21 AND 0.68
)
SELECT
    id,
    price,
    qty_share,
    customer_id_stddev_samp_qty_share_2,
    customer_id_max_age,
    qty_share_4,
    customer_id_max_age_tier,
    resolved_segment
FROM cleaned
ORDER BY price;
