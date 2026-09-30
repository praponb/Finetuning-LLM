WITH remaining_keys AS (
    -- Set difference on keys
    SELECT carrier, order_id
    FROM shipments
    EXCEPT
    SELECT carrier, order_id
    FROM shipments
    WHERE shipped_date < '2025-07-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM shipments s
    JOIN remaining_keys k ON s.carrier = k.carrier AND s.order_id = k.order_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        status,
        COUNT(cost) OVER (PARTITION BY carrier, status) AS carrier_count_cost,
        DENSE_RANK() OVER w_recent AS cost_dense_rank
    FROM kept_rows
    WHERE YEAR(shipped_date) >= 2025
    WINDOW w_recent AS (ORDER BY cost NULLS FIRST)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        order_id,
        shipped_date,
        delivered_date,
        status,
        cost_dense_rank,
        RANK() OVER (PARTITION BY status, order_id ORDER BY carrier_count_cost NULLS FIRST) AS carrier_count_cost_rank,
        PERCENT_RANK() OVER (ORDER BY carrier_count_cost DESC) AS carrier_count_cost_percent_rank,
        RANK() OVER (ORDER BY carrier_count_cost) AS carrier_count_cost_rank_2,
        COUNT(carrier_count_cost) OVER (PARTITION BY order_id) AS order_id_count_carrier_count_cost
    FROM metrics
),
remaining_keys_2 AS (
    -- Set difference on keys
    SELECT status, order_id
    FROM enriched
    EXCEPT
    SELECT status, order_id
    FROM shipments
    WHERE carrier = 'Kerry'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys_2 k ON s.status = k.status AND s.order_id = k.order_id
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.order_id,
        src.status,
        src.cost_dense_rank,
        src.carrier_count_cost_rank,
        src.carrier_count_cost_percent_rank,
        src.order_id_count_carrier_count_cost,
        b.order_date
    FROM reconciled src
    LEFT JOIN orders b
        ON src.order_id = b.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        status,
        order_id_count_carrier_count_cost
    FROM joined
) src
PIVOT (
    SUM(order_id_count_carrier_count_cost) AS amount,
    COUNT(1) AS item_count
    FOR status IN ('LOST', 'IN_TRANSIT', 'DELIVERED')
)
