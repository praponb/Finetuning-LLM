WITH FilteredOrderItems AS (
    SELECT *
    FROM order_items
    WHERE quantity BETWEEN 4 AND 6
),
quantity_bucket_summary AS (
    SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(*) AS total_count
    FROM FilteredOrderItems
    GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM quantity_bucket_summary
