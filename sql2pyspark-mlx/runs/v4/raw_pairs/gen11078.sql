WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, DATE_TRUNC('MONTH', shipped_date) AS shipped_date_period
    FROM shipments
    WHERE delivered_date BETWEEN '2023-11-25' AND '2025-09-12'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date_period, id) AS rank_in_group
        FROM valid
    ) t
    WHERE rank_in_group = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY cost DESC, id) AS rank_in_group_2
    FROM latest
    QUALIFY rank_in_group_2 = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT s.*, b.status AS order_status
    FROM deduped s
    LEFT JOIN orders b
        ON s.order_id = b.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, carrier
    FROM with_dims
    EXCEPT
    SELECT status, carrier
    FROM shipments
    WHERE delivered_date < '2025-05-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN remaining_keys k ON s.status = k.status AND s.carrier = k.carrier
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, rank_in_group - cost AS rank_in_group_minus_cost
    FROM reconciled
    WHERE delivered_date > DATE '2023-08-15'
      AND order_status IS NOT NULL
),
cleaned_shipments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        shipped_date_period,
        rank_in_group,
        rank_in_group_2,
        order_status,
        rank_in_group_minus_cost,
        DATE_TRUNC('YEAR', delivered_date) AS delivered_date_period
    FROM cleaned
    WHERE delivered_date > TO_DATE('2023-12-03')
      AND delivered_date > '2023-04-01'
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        order_status,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        COUNT(DISTINCT order_id) AS distinct_order_ids,
        APPROX_PERCENTILE(cost, 0.95) AS cost_p95,
        (UNIX_TIMESTAMP(MAX(delivered_date_period)) - UNIX_TIMESTAMP(MIN(delivered_date_period))) / 60 AS duration_min,
        SUM(rank_in_group_minus_cost) FILTER (WHERE carrier = 'FedEx') AS fedex_total_rank_in_group_minus_cost,
        COUNT(DISTINCT id) AS distinct_ids
    FROM cleaned_shipments
    GROUP BY order_status
)
SELECT
    order_status,
    status_list,
    duration_min,
    distinct_ids,
    CASE WHEN order_status RLIKE '^(PENDING|COMPLETED)$' THEN 'GROUP_A' WHEN order_status RLIKE '^(CANCELLED|SHIPPED)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS order_status_zone
FROM totals
ORDER BY order_status NULLS FIRST, duration_min;
