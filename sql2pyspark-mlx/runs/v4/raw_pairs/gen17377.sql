WITH cleaned AS (
    SELECT *, CASE WHEN status IN ('COMPLETED', 'RETURNED') THEN 'Tier 1' ELSE 'Other' END AS status_group
    FROM orders
    WHERE order_date >= DATE '2023-06-14'
      AND customer_id IS NOT NULL
)
SELECT *
FROM (
    SELECT
        store_id,
        channel,
        shipping_fee
    FROM cleaned
) src
PIVOT (
    AVG(shipping_fee) AS value_sum,
    COUNT(1) AS cnt
    FOR channel IN ('store' AS store, 'web' AS web)
);
