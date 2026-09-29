SELECT
    carrier,
    DATE_FORMAT(delivered_date, 'yyyy-MM') AS delivered_month,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    MAX(delivered_date) AS last_delivered_date
FROM shipments
GROUP BY carrier, DATE_FORMAT(delivered_date, 'yyyy-MM')
ORDER BY last_delivered_date
