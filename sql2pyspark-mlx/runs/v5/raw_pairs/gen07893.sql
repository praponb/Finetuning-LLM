WITH cleaned AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        CASE WHEN cost >= 74 THEN 'High' WHEN cost >= 23 THEN 'Medium' ELSE 'Low' END AS cost_tier,
        COALESCE(carrier, 'OTHER') AS carrier_filled
    FROM shipments
    WHERE carrier IS NOT NULL
      AND delivered_date > DATE '2023-02-28'
)
SELECT
    d.status AS order_status,
    src.carrier,
    COUNT(*) AS num_rows,
    MIN(src.cost) AS min_cost
FROM cleaned src
INNER JOIN orders d ON src.order_id = d.id
GROUP BY ROLLUP (d.status, src.carrier)
ORDER BY num_rows NULLS LAST
