WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        CONCAT_WS('-', carrier, status) AS carrier_status_key,
        DATE_TRUNC('MONTH', shipped_date) AS shipped_date_period
    FROM shipments
    WHERE order_id IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        delivered_date,
        cost,
        status,
        AVG(cost) OVER (PARTITION BY order_id, carrier ORDER BY CAST(shipped_date_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS cost_avg_30d,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) AS row_seq,
        RANK() OVER w_part AS cost_rank,
        CUME_DIST() OVER (ORDER BY cost NULLS LAST) AS cost_cume_dist
    FROM base
    WINDOW w_part AS (PARTITION BY order_id ORDER BY cost NULLS LAST)
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(cost) OVER w_grp AS next_cost,
        LAG(row_seq) OVER w_grp AS prev_row_seq,
        ROW_NUMBER() OVER w_grp AS row_seq_2,
        ROW_NUMBER() OVER w_grp_2 AS row_seq_3
    FROM enriched
    WINDOW w_grp AS (PARTITION BY status ORDER BY row_seq, id), w_grp_2 AS (PARTITION BY status ORDER BY cost, id)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY row_seq, id) AS row_num
        FROM enriched_shipments
    ) ranked
    WHERE row_num = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN cost_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS cost_rank_tier,
    CASE WHEN row_seq_2 >= 8 THEN 'HIGH' ELSE 'LOW' END AS row_seq_2_tier,
    CASE WHEN row_seq_3 >= 9 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_3_tier,
    COUNT_IF(status = 'RETURNED') AS returned_events,
    ROUND(SUM(cost_avg_30d) / NULLIF(COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END), 0), 2) AS cost_avg_30d_per_in_transit,
    SUM(CASE WHEN status = 'IN_TRANSIT' THEN row_seq_3 ELSE 0 END) AS in_transit_row_seq_3,
    GROUPING_ID() AS gid
FROM latest
GROUP BY GROUPING SETS ((CASE WHEN cost_rank >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN row_seq_2 >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN row_seq_3 >= 9 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN cost_rank >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN row_seq_2 >= 8 THEN 'HIGH' ELSE 'LOW' END))
HAVING COUNT_IF(status = 'RETURNED') >= 1;
