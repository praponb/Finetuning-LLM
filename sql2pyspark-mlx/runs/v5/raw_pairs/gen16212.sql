SELECT
    c.id,
    c.segment,
    COUNT(s.id) subscription_count,
    COUNT(s.monthly_fee) monthly_fee_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.status))) status_values
FROM customers c
LEFT JOIN subscriptions s ON c.id = s.customer_id
GROUP BY c.id, c.segment;
