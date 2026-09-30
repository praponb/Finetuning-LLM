WITH CustomerTotals AS (
    SELECT customer_id, COUNT(DISTINCT browser) AS unique_browsers, COUNT(CASE WHEN device = 'desktop' THEN 1 END) AS desktop_count
    FROM sessions
    WHERE customer_id = 8
    GROUP BY customer_id
)
SELECT
    c.id,
    c.country,
    C.unique_browsers,
    C.desktop_count
FROM customers c
LEFT JOIN CustomerTotals C ON c.id = C.customer_id
