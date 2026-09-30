WITH Base AS (
    SELECT *
    FROM shipments
    WHERE carrier NOT IN ('UPS', 'Kerry', 'DHL') AND carrier IS NULL
),
agg AS (
    SELECT status, SORT_ARRAY(COLLECT_SET(status)) AS status_list, COUNT(DISTINCT cost) AS unique_carriers
    FROM Base
    GROUP BY status
)
SELECT status, status_list, unique_carriers
FROM agg
WHERE unique_carriers > 8
ORDER BY status;
