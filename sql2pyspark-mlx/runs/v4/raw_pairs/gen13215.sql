WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(cost, TRUE) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost,
        AVG(cost) OVER w_recent AS cost_avg_6_prior
    FROM shipments
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
kept_keys AS (
    -- Set difference on keys
    SELECT order_id, id
    FROM enriched
    EXCEPT DISTINCT
    SELECT order_id, id
    FROM shipments
    WHERE order_id = 14
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.id = k.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(last_known_cost) OVER (PARTITION BY order_id, status) AS order_id_sum_last_known_cost,
        RANK() OVER (ORDER BY cost_avg_6_prior NULLS FIRST) AS cost_avg_6_prior_rank
    FROM reconciled
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        status,
        cost_avg_6_prior
    FROM with_windows
) src
PIVOT (
    MAX(cost_avg_6_prior)
    FOR status IN ('LOST' AS lost, 'IN_TRANSIT' AS in_transit, 'RETURNED' AS returned)
);
