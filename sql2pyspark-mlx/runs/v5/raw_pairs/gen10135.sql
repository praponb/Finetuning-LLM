WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        order_date,
        channel,
        AVG(shipping_fee) OVER (PARTITION BY store_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_6,
        AVG(discount) OVER (PARTITION BY customer_id ORDER BY discount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS discount_avg_5,
        NTILE(3) OVER (ORDER BY shipping_fee, id) AS shipping_fee_bucket,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY shipping_fee DESC, id) AS seq_no,
        NTILE(5) OVER (PARTITION BY channel ORDER BY discount, id) AS discount_bucket
    FROM orders
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_date DESC, id) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount_avg_5, id) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        channel,
        discount_avg_5,
        seq_no,
        discount_bucket,
        row_num,
        SUM(discount_avg_5) OVER w_ordered AS customer_id_sum_discount_avg_5,
        RANK() OVER (PARTITION BY customer_id ORDER BY discount_avg_5 DESC NULLS FIRST) AS discount_avg_5_rank,
        SUM(seq_no) OVER (PARTITION BY channel ORDER BY shipping_fee_avg_6, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_seq_no
    FROM deduped
    WINDOW w_ordered AS (PARTITION BY customer_id)
),
joined AS (
    -- Attach customers attributes
    SELECT e.*, p.segment
    FROM scored e
    LEFT JOIN customers p
        ON e.customer_id = p.id
    WHERE p.is_active
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    COUNT(*) FILTER (WHERE channel = 'partner') AS partner_rows,
    COUNT(*) FILTER (WHERE channel = 'mobile') AS mobile_rows,
    COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count
FROM joined
GROUP BY customer_id
HAVING COUNT(*) FILTER (WHERE channel = 'partner') >= 1
ORDER BY mobile_rows;
