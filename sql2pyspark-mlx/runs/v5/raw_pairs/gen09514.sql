WITH cleaned AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date
    FROM orders
    WHERE order_date >= DATE '2023-03-10'
      AND order_date BETWEEN '2023-11-01' AND '2024-05-01'
)
SELECT
    *,
    SUM(days_since_order_date) OVER w_ordered AS cumulative_days_since_order_date,
    CORR(amount, discount) OVER (PARTITION BY channel, store_id) AS corr_amount_discount
FROM cleaned
WHERE customer_id IS NULL
WINDOW w_ordered AS (PARTITION BY store_id ORDER BY days_since_order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
