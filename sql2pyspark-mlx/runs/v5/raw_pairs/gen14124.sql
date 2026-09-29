WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY store_id, customer_id), 4) AS amount_share,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY amount, id) AS amount_bucket,
        NTILE(3) OVER (PARTITION BY customer_id, store_id ORDER BY discount, id) AS discount_bucket,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY shipping_fee DESC NULLS LAST) AS shipping_fee_dense_rank
    FROM orders
)
-- Grouped metrics
SELECT
    CASE WHEN seq_no >= 9 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
    COUNT(DISTINCT status) AS distinct_status,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(DISTINCT store_id) AS distinct_store_ids
FROM with_windows
GROUP BY CUBE (CASE WHEN seq_no >= 9 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY seq_no_tier
