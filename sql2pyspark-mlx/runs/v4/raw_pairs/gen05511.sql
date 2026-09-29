SELECT
    YEAR(shipped_date) AS shipped_year,
    order_id,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list,
    MAX(cost) AS max_cost
FROM shipments
WHERE delivered_date >= '2024-03-28'
GROUP BY YEAR(shipped_date), order_id
HAVING COUNT(*) > 1
ORDER BY max_cost;
