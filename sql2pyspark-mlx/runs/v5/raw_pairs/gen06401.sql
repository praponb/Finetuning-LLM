WITH latest AS (
    SELECT
        id,
        order_id,
        carrier,
        delivered_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC NULLS LAST, id) AS dedup_rank
        FROM shipments
    ) t
    WHERE dedup_rank = 1
)
SELECT
    DATE_TRUNC('MONTH', delivered_date) AS activity_month,
    carrier,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids,
    RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', delivered_date) ORDER BY COUNT(*) DESC) AS row_count_rank
FROM latest
GROUP BY 1, 2
ORDER BY row_count DESC NULLS LAST;
