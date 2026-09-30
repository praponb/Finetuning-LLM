WITH joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.quantity,
        src.unit_price,
        p.status
    FROM order_items src
    LEFT JOIN orders p
        ON src.order_id = p.id
)
-- Grouped metrics
SELECT
    status,
    order_id,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending_count,
    RANK() OVER (PARTITION BY status ORDER BY COUNT(CASE WHEN status = 'PENDING' THEN 1 END) DESC) AS pending_count_rank
FROM joined
GROUP BY 1, 2
ORDER BY status NULLS LAST;
