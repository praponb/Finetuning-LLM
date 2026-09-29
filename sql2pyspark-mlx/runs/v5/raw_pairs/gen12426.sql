WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_date,
        channel,
        amount,
        shipping_fee,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        LAG(amount, 1) OVER (PARTITION BY customer_id, store_id ORDER BY shipping_fee, id) AS prev_amount,
        SUM(shipping_fee) OVER w_ordered_2 AS cumulative_shipping_fee,
        LEAD(discount, 2) OVER w_hist AS next_discount,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount, id) AS row_seq
    FROM orders
    WINDOW w_ordered AS (ORDER BY discount, id), w_ordered_2 AS (PARTITION BY status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY channel ORDER BY order_date, id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_date,
        amount,
        cumulative_shipping_fee,
        next_discount,
        AVG(cumulative_shipping_fee) OVER (PARTITION BY channel ORDER BY order_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cumulative_shipping_fee_avg_3,
        PERCENT_RANK() OVER (PARTITION BY channel ORDER BY row_seq NULLS LAST) AS row_seq_percent_rank,
        CORR(shipping_fee, amount) OVER (PARTITION BY channel) AS corr_shipping_fee_amount
    FROM metrics
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(amount, 2) AS amount_rounded,
        cumulative_shipping_fee * cumulative_shipping_fee_avg_3 AS cumulative_shipping_fee_times_cumulative_shipping_fee_avg_3
    FROM with_windows
    WHERE order_date >= DATE '2023-03-27'
      AND cumulative_shipping_fee > 197
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        amount,
        next_discount,
        cumulative_shipping_fee_avg_3,
        row_seq_percent_rank,
        corr_shipping_fee_amount,
        cumulative_shipping_fee_times_cumulative_shipping_fee_avg_3,
        ROUND(corr_shipping_fee_amount, 1) AS corr_shipping_fee_amount_rounded,
        cumulative_shipping_fee_times_cumulative_shipping_fee_avg_3 * cumulative_shipping_fee_avg_3 AS cumulative_shipping_fee_times_cumulative_shipping_fee_avg_3_times_cumulative_shipping_fee_avg_3
    FROM base
    WHERE order_date > DATE '2023-09-03'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM filtered
    EXCEPT ALL
    SELECT id
    FROM orders
    WHERE amount BETWEEN 470 AND 860
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN cumulative_shipping_fee_avg_3 >= 224 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_shipping_fee_avg_3_tier,
    APPROX_PERCENTILE(corr_shipping_fee_amount_rounded, 0.9) AS corr_shipping_fee_amount_rounded_p90,
    ROUND(AVG(cumulative_shipping_fee_times_cumulative_shipping_fee_avg_3), 2) AS avg_cumulative_shipping_fee_times_cumulative_shipping_fee_avg_3,
    APPROX_PERCENTILE(corr_shipping_fee_amount, 0.9) AS corr_shipping_fee_amount_p90
FROM reconciled
GROUP BY CASE WHEN cumulative_shipping_fee_avg_3 >= 224 THEN 'LARGE' ELSE 'SMALL' END
HAVING APPROX_PERCENTILE(corr_shipping_fee_amount, 0.9) < 1000000
ORDER BY avg_cumulative_shipping_fee_times_cumulative_shipping_fee_avg_3;
