WITH src AS (
    SELECT *, CASE WHEN country = 'JP' THEN 1 ELSE 0 END AS is_jp
    FROM customers
    WHERE NOT (credit_limit > 5500 AND NOT is_active)
),
segment_summary AS (
    SELECT segment, SUM(CASE WHEN city = 'Singapore' THEN credit_limit ELSE 0 END) AS singapore_credit_limit, COUNT(*) AS row_count
    FROM src
    GROUP BY segment
)
SELECT segment, singapore_credit_limit, row_count
FROM segment_summary
ORDER BY row_count, segment;
