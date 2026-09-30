WITH valid AS (
    SELECT *, LOWER(status) AS status_lower
    FROM shipments
    WHERE delivered_date >= DATE '2023-12-25'
      AND order_id IS NOT NULL
      AND order_id = 6
)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost, id) AS latest_rank
    FROM valid
) ranked
WHERE latest_rank = 1;
