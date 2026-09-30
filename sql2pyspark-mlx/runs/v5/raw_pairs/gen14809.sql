WITH cleaned AS (
    SELECT
        *,
        LEAST(amount, shipping_fee) AS least_amount_shipping_fee,
        CASE WHEN discount >= 0.43 THEN 'High' WHEN discount >= 0.12 THEN 'Medium' ELSE 'Low' END AS discount_band
    FROM orders
    WHERE order_date > TO_DATE('2023-08-26')
)
SELECT
    id,
    customer_id,
    metric,
    metric_value
FROM (
    SELECT
        id,
        customer_id,
        discount,
        amount,
        shipping_fee
    FROM cleaned
) src
UNPIVOT (
    metric_value FOR metric IN (discount, amount, shipping_fee)
);
