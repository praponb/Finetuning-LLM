WITH top_shipments AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY carrier ORDER BY delivered_date DESC) AS rnk
    FROM shipments
    WHERE delivered_date BETWEEN '2023-05-01' AND '2023-05-02'
)
SELECT id, carrier, delivered_date
FROM top_shipments
WHERE rnk <= 2
ORDER BY carrier DESC;
