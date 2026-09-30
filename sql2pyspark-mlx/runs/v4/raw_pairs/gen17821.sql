WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        DATE_FORMAT(delivered_date, 'yyyy-MM-dd') AS delivered_date_label,
        DATE_TRUNC('YEAR', shipped_date) AS shipped_date_period,
        COALESCE(carrier, 'OTHER') AS carrier_clean
    FROM shipments
    WHERE delivered_date >= DATE '2023-09-21'
      AND carrier IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN shipped_date_period - LAG(shipped_date_period) OVER (PARTITION BY carrier ORDER BY shipped_date_period ASC, id ASC) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY carrier ORDER BY shipped_date_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        delivered_date_label,
        shipped_date_period,
        carrier_clean,
        is_new_burst,
        session_id,
        FIRST_VALUE(carrier, TRUE) OVER (PARTITION BY status ORDER BY shipped_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_carrier,
        AVG(cost) OVER (PARTITION BY carrier ORDER BY CAST(shipped_date_period AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS cost_avg_90d,
        LAG(is_new_burst, 2) OVER (PARTITION BY status ORDER BY cost ASC, id ASC) AS prev_is_new_burst,
        MIN(session_id) OVER (PARTITION BY status) AS status_min_session_id,
        SUM(session_id) OVER (PARTITION BY status, carrier ORDER BY CAST(shipped_date_period AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS session_id_sum_30d
    FROM islands
),
kept_keys AS (
    -- Set intersection on keys
    SELECT status, id
    FROM enriched
    INTERSECT
    SELECT status, id
    FROM shipments
    WHERE MONTH(delivered_date) IN (3, 6, 8)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.status = k.status AND s.id = k.id
),
enriched_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY prev_is_new_burst ASC NULLS LAST) AS prev_is_new_burst_cume_dist,
        DENSE_RANK() OVER (PARTITION BY carrier ORDER BY cost ASC) AS cost_dense_rank,
        SUM(cost) OVER (ORDER BY CAST(shipped_date_period AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS cost_sum_30d
    FROM reconciled
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY carrier ORDER BY session_id_sum_30d DESC NULLS LAST) AS session_id_sum_30d_rank,
        MIN(session_id_sum_30d) OVER w_recent AS status_min_session_id_sum_30d,
        MAX(cost_dense_rank) OVER (ORDER BY CAST(shipped_date_period AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS cost_dense_rank_max_30d
    FROM enriched_shipments
    WINDOW w_recent AS (PARTITION BY status, carrier)
)
SELECT
    id,
    carrier,
    delivered_date,
    cost,
    status,
    delivered_date_label,
    shipped_date_period,
    carrier_clean,
    is_new_burst,
    CASE WHEN carrier RLIKE '^(UPS|Flash)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS carrier_zone
FROM scored
ORDER BY is_new_burst ASC NULLS LAST;
