WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(amount, discount) AS least_amount_discount
    FROM orders
    WHERE order_date > DATE '2024-02-10'
      AND channel IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER w_part AS shipping_fee_bucket,
        COUNT(discount) OVER (PARTITION BY status, customer_id) AS status_count_discount,
        ROW_NUMBER() OVER w_part AS seq_no,
        CORR(shipping_fee, amount) OVER (PARTITION BY status) AS corr_shipping_fee_amount,
        SUM(shipping_fee) OVER (PARTITION BY store_id) AS store_id_sum_shipping_fee
    FROM cleaned
    WINDOW w_part AS (PARTITION BY customer_id ORDER BY shipping_fee ASC, id ASC)
),
scored_orders AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY status ORDER BY amount DESC, id ASC) AS amount_bucket,
        COUNT(shipping_fee) OVER (PARTITION BY store_id) AS store_id_count_shipping_fee
    FROM scored
),
scored_orders_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(corr_shipping_fee_amount) OVER (ORDER BY store_id_sum_shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_corr_shipping_fee_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY status_count_discount ASC, id ASC) AS row_seq
    FROM scored_orders
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        least_amount_discount,
        shipping_fee_bucket,
        status_count_discount,
        seq_no,
        corr_shipping_fee_amount,
        store_id_sum_shipping_fee,
        amount_bucket,
        store_id_count_shipping_fee,
        running_corr_shipping_fee_amount,
        row_seq,
        AVG(status_count_discount) OVER w_part AS status_count_discount_avg_2,
        SUM(discount) OVER (PARTITION BY store_id, status ORDER BY amount_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount
    FROM scored_orders_2
    WINDOW w_part AS (PARTITION BY store_id ORDER BY store_id_count_shipping_fee ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.shipped_date < s.order_date
        )
)
-- Grouped metrics
SELECT
    CASE WHEN running_corr_shipping_fee_amount >= 4 THEN 'LARGE' ELSE 'SMALL' END AS running_corr_shipping_fee_amount_tier,
    COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count,
    MAX(seq_no) FILTER (WHERE status = 'RETURNED') AS returned_max_seq_no,
    COUNT(*) FILTER (WHERE channel = 'web') AS web_rows,
    SUM(CASE WHEN channel = 'mobile' THEN cumulative_discount ELSE 0 END) AS mobile_cumulative_discount
FROM kept
GROUP BY CASE WHEN running_corr_shipping_fee_amount >= 4 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY returned_max_seq_no ASC
