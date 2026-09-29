SELECT *
FROM (
    SELECT
        status,
        QUARTER(order_date) order_quarter,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values,
        COUNT(shipping_fee) shipping_fee_count
    FROM orders
    WHERE shipping_fee < 37
    GROUP BY status, QUARTER(order_date)
) sub
WHERE shipping_fee_count < 4
ORDER BY shipping_fee_count ASC, status ASC, order_quarter ASC
LIMIT 10
