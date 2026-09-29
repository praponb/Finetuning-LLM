SELECT id, discount, CASE WHEN channel IN ('mobile', 'partner') THEN 'Core' ELSE 'Other' END AS channel_group FROM orders WHERE customer_id IS NOT NULL ORDER BY channel_group;
