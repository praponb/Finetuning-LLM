WITH base AS (SELECT * FROM shipments WHERE cost < 65), agg AS (SELECT status, MAX(shipped_date) AS latest_shipped_date FROM base GROUP BY status) SELECT status, latest_shipped_date FROM agg
