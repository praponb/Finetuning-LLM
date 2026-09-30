WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        delivered_date,
        cost,
        status,
        CONCAT(carrier, ' ', status) AS carrier_status
    FROM shipments
    WHERE delivered_date > DATE '2023-04-09'
      AND carrier IS NOT NULL
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', delivered_date) AS year_start,
    status,
    SIZE(COLLECT_SET(status)) AS num_status,
    MAX(cost) AS max_cost
FROM cleaned
GROUP BY 1, 2
ORDER BY num_status ASC
