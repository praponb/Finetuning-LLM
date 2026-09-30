WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(carrier, TRUE) OVER (PARTITION BY status, carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier,
        RANK() OVER w_grp AS cost_rank,
        LAST_VALUE(carrier) IGNORE NULLS OVER w_recent AS last_known_carrier_2,
        DENSE_RANK() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS FIRST) AS cost_dense_rank
    FROM shipments
    WINDOW w_grp AS (ORDER BY cost), w_recent AS (ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', shipped_date) AS week_start,
    DATE_TRUNC('QUARTER', delivered_date) AS quarter_start,
    DATE_TRUNC('YEAR', shipped_date) AS year_start,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list
FROM with_windows
GROUP BY DATE_TRUNC('WEEK', shipped_date), DATE_TRUNC('QUARTER', delivered_date), DATE_TRUNC('YEAR', shipped_date)
ORDER BY returned_count NULLS FIRST
