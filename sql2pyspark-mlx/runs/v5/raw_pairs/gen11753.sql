WITH filtered AS (
    SELECT *, DATE_FORMAT(order_date, 'yyyy-MM-dd') order_date_label
    FROM orders
    WHERE customer_id IS NOT NULL
      AND order_date >= DATE '2023-10-07'
),
remaining_keys AS (
    SELECT id, status
    FROM filtered
    EXCEPT
    SELECT id, status
    FROM orders
    WHERE order_date < '2023-12-23'
),
verified AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.status = k.status
),
with_windows AS (
    SELECT
        *,
        CORR(amount, discount) OVER w_part corr_amount_discount,
        CORR(amount, shipping_fee) OVER (PARTITION BY customer_id, store_id) corr_amount_shipping_fee
    FROM verified
    WINDOW w_part AS (PARTITION BY channel)
),
metrics AS (
    SELECT
        customer_id,
        status,
        amount,
        discount,
        order_date_label,
        corr_amount_discount,
        CORR(amount, shipping_fee) OVER (PARTITION BY store_id, status) corr_amount_shipping_fee_2
    FROM with_windows
),
enriched AS (
    SELECT
        customer_id,
        status,
        order_date_label,
        corr_amount_discount,
        corr_amount_shipping_fee_2,
        AVG(corr_amount_discount) OVER (PARTITION BY status) status_avg_corr_amount_discount
    FROM metrics
),
stats AS (
    SELECT
        customer_id,
        CASE WHEN corr_amount_shipping_fee_2 >= 0.68 THEN 'PREMIUM' ELSE 'STANDARD' END AS corr_amount_shipping_fee_2_tier,
        ROUND(AVG(status_avg_corr_amount_discount), 2) AS avg_status_avg_corr_amount_discount,
        PERCENTILE_APPROX(corr_amount_discount, ARRAY(0.25, 0.5, 0.75)) AS corr_amount_discount_quartiles,
        SIZE(COLLECT_SET(status)) AS num_status
    FROM enriched
    GROUP BY customer_id, CASE WHEN corr_amount_shipping_fee_2 >= 0.68 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING ROUND(AVG(status_avg_corr_amount_discount), 2) > 0
)
SELECT
    s.customer_id,
    s.corr_amount_shipping_fee_2_tier,
    s.num_status,
    (SELECT SUM(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS sum_account_balance
FROM stats s;
