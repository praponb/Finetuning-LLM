WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        cost,
        DATE_TRUNC('month', delivered_date) delivered_date_period
    FROM shipments
    WHERE carrier IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN delivered_date_period - LAG(delivered_date_period) OVER (PARTITION BY carrier ORDER BY delivered_date_period, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY carrier ORDER BY delivered_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
remaining_keys AS (
    -- Set difference on keys
    SELECT carrier, id
    FROM sessionized
    EXCEPT DISTINCT
    SELECT carrier, id
    FROM shipments
    WHERE delivered_date <= '2025-03-04'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    INNER JOIN remaining_keys k ON s.carrier = k.carrier AND s.id = k.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(session_no) OVER (PARTITION BY carrier ORDER BY CAST(delivered_date_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) session_no_max_90d,
        PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS FIRST) cost_percent_rank,
        DENSE_RANK() OVER (PARTITION BY carrier ORDER BY session_no DESC) session_no_dense_rank
    FROM reconciled
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', delivered_date_period) AS activity_month,
    DATE_TRUNC('WEEK', delivered_date_period) AS week_start,
    COUNT(DISTINCT carrier) AS distinct_carriers,
    MIN(delivered_date_period) AS first_seen,
    (UNIX_TIMESTAMP(MAX(delivered_date_period)) - UNIX_TIMESTAMP(MIN(delivered_date_period))) / 60 AS span_minutes,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', delivered_date_period) ORDER BY COUNT(DISTINCT carrier) DESC) AS distinct_carriers_dense_rank
FROM scored
GROUP BY activity_month, week_start
ORDER BY activity_month NULLS LAST;
