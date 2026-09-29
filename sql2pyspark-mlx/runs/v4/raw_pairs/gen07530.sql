SELECT
    customer_id,
    CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort,
    MIN(amount) AS min_amount,
    SUM(discount) AS total_discount
FROM orders
WHERE amount <= 690 AND (discount < 0.29 OR shipping_fee < 33)
GROUP BY customer_id, order_cohort
