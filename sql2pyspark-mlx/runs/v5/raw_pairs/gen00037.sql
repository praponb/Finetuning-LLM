WITH rollup_totals AS (
    SELECT
        p.country,
        e.status,
        COUNT(*) AS num_rows,
        MAX(e.discount) AS max_discount,
        MAX(e.amount) AS max_amount
    FROM orders e
    INNER JOIN customers p ON e.customer_id = p.id
    GROUP BY GROUPING SETS ((p.country, e.status), (p.country), ())
)
SELECT
    num_rows,
    max_discount,
    max_amount
FROM rollup_totals
ORDER BY num_rows DESC
