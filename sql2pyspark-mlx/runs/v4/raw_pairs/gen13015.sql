WITH kept_keys AS (
    SELECT order_id, id
    FROM shipments
    INTERSECT
    SELECT order_id, id
    FROM shipments
    WHERE carrier = 'Kerry'
),
kept_rows AS (
    SELECT s.*
    FROM shipments s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.id = k.id
),
metrics AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        DENSE_RANK() OVER (PARTITION BY carrier, order_id ORDER BY cost NULLS LAST) AS cost_dense_rank,
        AVG(cost) OVER (PARTITION BY carrier) AS carrier_avg_cost
    FROM kept_rows
),
scored AS (
    SELECT
        *,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share,
        DENSE_RANK() OVER (ORDER BY cost DESC) AS cost_dense_rank_2
    FROM metrics
),
enriched AS (
    SELECT
        s.*,
        p.status AS order_status,
        p.order_date
    FROM scored s
    LEFT JOIN orders p
        ON s.order_id = p.id
),
scored_shipments AS (
    SELECT
        *,
        COUNT(cost) OVER (PARTITION BY status) AS status_count_cost,
        PERCENT_RANK() OVER (ORDER BY cost_share DESC NULLS LAST) AS cost_share_percent_rank,
        SUM(cost_share) OVER (PARTITION BY order_id) AS order_id_sum_cost_share
    FROM enriched
),
with_windows AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_dense_rank,
        carrier_avg_cost,
        cost_share,
        cost_dense_rank_2,
        order_status,
        order_date,
        status_count_cost,
        cost_share_percent_rank,
        order_id_sum_cost_share,
        MAX(status_count_cost) OVER (PARTITION BY carrier) AS carrier_max_status_count_cost,
        MAX(cost_dense_rank_2) OVER (PARTITION BY status) AS status_max_cost_dense_rank_2,
        AVG(cost_share) OVER (PARTITION BY order_id) AS order_id_avg_cost_share
    FROM scored_shipments
),
scored_shipments_2 AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_dense_rank,
        carrier_avg_cost,
        cost_share,
        cost_dense_rank_2,
        order_status,
        order_date,
        status_count_cost,
        cost_share_percent_rank,
        order_id_sum_cost_share,
        carrier_max_status_count_cost,
        status_max_cost_dense_rank_2,
        order_id_avg_cost_share,
        ROUND(cost_dense_rank / SUM(cost_dense_rank) OVER (PARTITION BY carrier, status), 4) AS cost_dense_rank_share,
        AVG(cost_share) OVER (PARTITION BY order_status) AS order_status_avg_cost_share,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY carrier_avg_cost DESC NULLS FIRST) AS carrier_avg_cost_percent_rank
    FROM with_windows
)
SELECT
    id,
    order_id,
    carrier,
    delivered_date,
    status,
    cost_dense_rank,
    carrier_avg_cost,
    cost_share,
    order_status
FROM scored_shipments_2;
