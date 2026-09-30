WITH enriched AS (
    SELECT
        e.*,
        b.status,
        b.discount
    FROM order_items e
    LEFT JOIN orders b
        ON e.order_id = b.id
)
SELECT
    CASE WHEN quantity >= 8 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    SIZE(COLLECT_SET(status)) AS num_status,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list_2
FROM enriched
GROUP BY CASE WHEN quantity >= 8 THEN 'LARGE' ELSE 'SMALL' END
HAVING SIZE(COLLECT_SET(status)) > 1;
