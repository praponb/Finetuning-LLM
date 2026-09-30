SELECT
    id,
    discount,
    DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date,
    CASE WHEN status IN ('SHIPPED', 'RETURNED') THEN 'Group A' ELSE 'Other' END AS status_group
FROM orders
WHERE shipping_fee > 40;
