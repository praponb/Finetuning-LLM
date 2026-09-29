WITH rollup_totals AS (
    SELECT
        d.channel,
        e.carrier,
        COUNT(*) AS rows_total,
        MIN(e.cost) AS min_cost,
        GROUPING_ID() AS gid
    FROM shipments e
    JOIN orders d ON e.order_id = d.id
    GROUP BY ROLLUP (d.channel, e.carrier)
)
SELECT
    channel,
    rows_total,
    min_cost,
    gid
FROM rollup_totals
ORDER BY gid DESC NULLS LAST, channel
