WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY unit_price DESC NULLS LAST, id) AS dedup_rank
        FROM order_items
    ) ranked
    WHERE dedup_rank = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.amount,
        p.status,
        p.discount
    FROM latest e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        unit_price,
        amount,
        LAST_VALUE(discount) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount,
        CUME_DIST() OVER (ORDER BY quantity NULLS FIRST) AS quantity_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) AS seq_no
    FROM joined
    WHERE status IN ('SHIPPED', 'CANCELLED')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        unit_price,
        last_known_discount,
        quantity_cume_dist,
        LAST_VALUE(amount) IGNORE NULLS OVER w_running AS last_known_amount,
        AVG(last_known_discount) OVER (PARTITION BY product_id) AS product_id_avg_last_known_discount,
        LAG(unit_price, 2) OVER w_running_2 AS prev_unit_price,
        SUM(quantity_cume_dist) OVER (PARTITION BY product_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_cume_dist,
        LAG(unit_price, 1) OVER (PARTITION BY product_id ORDER BY last_known_discount, id) AS prev_unit_price_2
    FROM with_windows
    WINDOW w_running AS (PARTITION BY product_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running_2 AS (PARTITION BY product_id ORDER BY unit_price, id)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        last_known_discount,
        quantity_cume_dist,
        product_id_avg_last_known_discount,
        prev_unit_price_2,
        NTILE(5) OVER (ORDER BY product_id_avg_last_known_discount DESC, id) AS product_id_avg_last_known_discount_bucket,
        RANK() OVER (PARTITION BY product_id ORDER BY prev_unit_price_2 DESC NULLS FIRST) AS prev_unit_price_2_rank
    FROM enriched
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN prev_unit_price_2_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_unit_price_2_rank_tier,
        AVG(quantity_cume_dist) AS avg_quantity_cume_dist,
        ROUND(AVG(last_known_discount), 2) AS avg_last_known_discount,
        APPROX_PERCENTILE(product_id_avg_last_known_discount, 0.5) AS product_id_avg_last_known_discount_p50,
        COUNT(DISTINCT product_id) AS distinct_product_ids,
        COUNT(DISTINCT product_id) AS distinct_product_ids_2
    FROM scored
    GROUP BY 1
    HAVING COUNT(DISTINCT product_id) > 1
)
SELECT
    prev_unit_price_2_rank_tier,
    avg_quantity_cume_dist,
    avg_last_known_discount,
    product_id_avg_last_known_discount_p50,
    distinct_product_ids
FROM totals
ORDER BY avg_quantity_cume_dist DESC
