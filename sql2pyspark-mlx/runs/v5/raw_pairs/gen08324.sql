WITH base AS (
    SELECT *
    FROM shipments
    WHERE cost > 50
),
agg AS (
    SELECT YEAR(shipped_date) AS shipped_year, COUNT(cost) AS cost_count, COUNT(DISTINCT cost) AS unique_statuss
    FROM base
    GROUP BY YEAR(shipped_date)
)
SELECT shipped_year, cost_count, unique_statuss
FROM agg
WHERE cost_count >= 7
