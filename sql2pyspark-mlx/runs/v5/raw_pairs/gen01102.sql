SELECT
    id,
    status,
    shipping_fee,
    CASE WHEN channel IN ('store', 'web') THEN 'Tier 1' ELSE 'Other' END AS channel_group
FROM orders
WHERE order_date BETWEEN '2023-08-01' AND '2024-04-01' AND discount BETWEEN 0.18 AND 0.33
ORDER BY status, shipping_fee DESC;
