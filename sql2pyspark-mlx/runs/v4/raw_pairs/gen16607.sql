WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        status,
        channel,
        amount,
        shipping_fee,
        discount / NULLIF(shipping_fee, 0) AS discount_per_shipping_fee,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM orders
    WHERE customer_id IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT store_id, id
    FROM filtered
    EXCEPT DISTINCT
    SELECT store_id, id
    FROM orders
    WHERE channel IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.store_id = k.store_id AND s.id = k.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        store_id,
        status,
        channel,
        amount,
        shipping_fee,
        discount_per_shipping_fee,
        channel_clean,
        MIN(discount_per_shipping_fee) OVER (PARTITION BY customer_id) AS customer_id_min_discount_per_shipping_fee,
        CORR(shipping_fee, amount) OVER (PARTITION BY store_id, customer_id) AS corr_shipping_fee_amount,
        AVG(shipping_fee) OVER w_hist AS channel_avg_shipping_fee,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY shipping_fee DESC NULLS FIRST) AS shipping_fee_dense_rank,
        DENSE_RANK() OVER w_running AS discount_per_shipping_fee_dense_rank
    FROM kept_rows
    WINDOW w_hist AS (PARTITION BY channel), w_running AS (PARTITION BY channel, store_id ORDER BY discount_per_shipping_fee DESC NULLS LAST)
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.*,
        d.opened_date,
        d.sqft
    FROM with_windows e
    LEFT JOIN stores d
        ON e.store_id = d.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    ROUND(AVG(discount_per_shipping_fee_dense_rank), 2) AS avg_discount_per_shipping_fee_dense_rank,
    SUM(CASE WHEN channel = 'partner' THEN amount ELSE 0 END) AS partner_amount,
    COUNT(*) FILTER (WHERE status = 'SHIPPED') AS shipped_rows,
    MAX(shipping_fee) AS max_shipping_fee
FROM enriched
GROUP BY year_start
ORDER BY max_shipping_fee DESC;
