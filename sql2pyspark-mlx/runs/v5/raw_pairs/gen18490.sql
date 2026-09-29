WITH rollup_totals AS (
    SELECT
        d.region,
        d.city,
        COUNT(*) AS num_rows,
        MAX(src.quantity) AS max_quantity
    FROM inventory src
    JOIN stores d ON src.store_id = d.id
    GROUP BY GROUPING SETS ((d.region, d.city), (d.region), ())
)
SELECT
    region,
    num_rows,
    max_quantity
FROM rollup_totals
ORDER BY region DESC NULLS LAST;
