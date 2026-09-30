SELECT
    channel,
    status_group,
    stddev_discount,
    last_order_date
FROM (
    SELECT
        channel,
        CASE WHEN status IN ('CANCELLED', 'RETURNED') THEN 'priority' ELSE 'other' END AS status_group,
        ROUND(STDDEV(discount), 2) AS stddev_discount,
        MAX(order_date) AS last_order_date
    FROM orders
    WHERE status = 'PENDING'
    GROUP BY channel, CASE WHEN status IN ('CANCELLED', 'RETURNED') THEN 'priority' ELSE 'other' END
) AS t
WHERE stddev_discount > 58;
