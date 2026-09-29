WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(carrier, ', ', status) AS carrier_status,
        COALESCE(carrier, 'UNKNOWN') AS carrier_filled
    FROM shipments
    WHERE delivered_date >= DATE '2023-09-02'
      AND order_id IS NOT NULL
      AND cost BETWEEN 51 AND 62
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        carrier_status,
        carrier_filled,
        SUM(cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        SUM(cost) OVER w_hist AS running_cost_2,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date, id) AS seq_no
    FROM filtered
    WINDOW w_hist AS (PARTITION BY carrier, order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(seq_no) IGNORE NULLS OVER (PARTITION BY status ORDER BY running_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no,
        LAST_VALUE(running_cost_2) IGNORE NULLS OVER (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_cost_2
    FROM scored
),
kept_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM with_windows
    EXCEPT ALL
    SELECT status, id
    FROM shipments
    WHERE YEAR(delivered_date) = 2025
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.status = k.status AND s.id = k.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        b.shipping_fee,
        b.amount
    FROM kept_rows s
    INNER JOIN orders b
        ON s.order_id = b.id
),
scored_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY running_cost_2 NULLS FIRST) AS running_cost_2_percent_rank,
        ROUND(running_cost_2 / SUM(running_cost_2) OVER (PARTITION BY order_id), 4) AS running_cost_2_share,
        CUME_DIST() OVER (ORDER BY running_cost_2 NULLS LAST) AS running_cost_2_cume_dist,
        ROUND(first_known_seq_no / SUM(first_known_seq_no) OVER (PARTITION BY status), 4) AS first_known_seq_no_share
    FROM with_dims
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.shipped_date,
    s.delivered_date,
    s.cost,
    s.status,
    s.carrier_filled,
    s.running_cost,
    CASE WHEN carrier RLIKE '^(DHL|UPS)$' THEN 'GROUP_B' WHEN carrier RLIKE '^(FedEx|Kerry)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS carrier_zone,
    (SELECT MIN(x.amount) FROM payments x WHERE x.order_id = s.order_id) AS min_payment_amount
FROM scored_shipments s
ORDER BY status DESC;
