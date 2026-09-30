WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(shipping_fee, 1) AS shipping_fee_rounded,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM orders
    WHERE order_date >= DATE '2023-08-11'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, status
    FROM base
    EXCEPT
    SELECT customer_id, status
    FROM orders
    WHERE customer_id IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.status = k.status
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        order_date,
        status,
        amount,
        shipping_fee,
        resolved_channel,
        COALESCE(shipping_fee_rounded, 0) AS shipping_fee_rounded_filled,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM reconciled
    WHERE order_date > DATE '2024-01-13'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        order_date,
        amount,
        resolved_channel,
        shipping_fee_rounded_filled,
        LAST_DAY(order_date) AS order_date_month_end,
        CASE WHEN shipping_fee_rounded_filled >= 89 THEN 'High' WHEN shipping_fee_rounded_filled >= 28 THEN 'Medium' ELSE 'Low' END AS shipping_fee_rounded_filled_band
    FROM filtered
    WHERE customer_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY customer_id) AS customer_id_avg_amount,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY amount) AS amount_percent_rank,
        DENSE_RANK() OVER (ORDER BY amount NULLS FIRST) AS amount_dense_rank,
        AVG(shipping_fee_rounded_filled) OVER (PARTITION BY customer_id) AS customer_id_avg_shipping_fee_rounded_filled,
        DENSE_RANK() OVER (ORDER BY shipping_fee_rounded_filled DESC) AS shipping_fee_rounded_filled_dense_rank
    FROM cleaned
),
enriched AS (
    -- Ranking and gap features
    SELECT
        customer_id,
        order_date,
        amount,
        resolved_channel,
        shipping_fee_rounded_filled,
        order_date_month_end,
        shipping_fee_rounded_filled_band,
        customer_id_avg_amount,
        amount_percent_rank,
        amount_dense_rank,
        customer_id_avg_shipping_fee_rounded_filled,
        shipping_fee_rounded_filled_dense_rank,
        SUM(amount) OVER (PARTITION BY customer_id) AS customer_id_sum_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY customer_id), 4) AS amount_share,
        DENSE_RANK() OVER (ORDER BY customer_id_avg_shipping_fee_rounded_filled DESC) AS customer_id_avg_shipping_fee_rounded_filled_dense_rank
    FROM scored
),
with_dims AS (
    -- Attach customers attributes
    SELECT s.*, d.signup_date
    FROM enriched s
    LEFT JOIN customers d
        ON s.customer_id = d.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN shipping_fee_rounded_filled_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS shipping_fee_rounded_filled_dense_rank_tier,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    ROUND(AVG(customer_id_avg_shipping_fee_rounded_filled_dense_rank), 2) AS avg_customer_id_avg_shipping_fee_rounded_filled_dense_rank,
    PERCENTILE_APPROX(shipping_fee_rounded_filled_dense_rank, 0.5) AS shipping_fee_rounded_filled_dense_rank_p50
FROM with_dims
GROUP BY CASE WHEN shipping_fee_rounded_filled_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END
HAVING avg_customer_id_avg_shipping_fee_rounded_filled_dense_rank > 0
ORDER BY avg_customer_id_avg_shipping_fee_rounded_filled_dense_rank DESC;
