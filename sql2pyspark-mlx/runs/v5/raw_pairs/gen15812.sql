WITH valid AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        ROUND(amount, 2) AS amount_rounded
    FROM orders
    WHERE order_date >= TO_DATE('2023-04-17')
      AND channel IS DISTINCT FROM 'mobile'
)
SELECT
    DATE_TRUNC('WEEK', order_date) AS week_start,
    STDDEV_SAMP(discount) AS discount_stddev,
    PERCENTILE_APPROX(amount_rounded, 0.9) AS amount_rounded_p90,
    STDDEV_SAMP(discount) AS discount_stddev_2
FROM valid
GROUP BY week_start
ORDER BY amount_rounded_p90 DESC;
