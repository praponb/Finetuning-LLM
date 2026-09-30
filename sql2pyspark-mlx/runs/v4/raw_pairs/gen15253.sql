SELECT
    customer_id,
    CORR(amount, shipping_fee) AS corr_amount_shipping_fee,
    SUM(amount) FILTER (WHERE channel = 'store') AS store_total_amount,
    ROUND(AVG(discount), 2) AS avg_discount
FROM orders
GROUP BY customer_id
HAVING SUM(amount) FILTER (WHERE channel = 'store') >= 0;
