WITH cleaned AS (
    SELECT
        *,
        COALESCE(carrier, 'unassigned') AS carrier_filled,
        DATE_FORMAT(shipped_date, 'yyyy-MM-dd') AS shipped_date_label
    FROM shipments
    WHERE shipped_date > DATE '2023-04-25'
      AND carrier IS NOT NULL
),
latest AS (
    SELECT
        id,
        order_id,
        carrier,
        cost
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier, order_id ORDER BY cost DESC, id) = 1
),
with_windows AS (
    SELECT
        id,
        order_id,
        FIRST_VALUE(carrier, TRUE) OVER (PARTITION BY carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_carrier,
        CUME_DIST() OVER (PARTITION BY carrier, order_id ORDER BY cost) AS cost_cume_dist
    FROM latest
),
scored AS (
    SELECT
        *,
        SUM(cost_cume_dist) OVER (PARTITION BY order_id ORDER BY cost_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_cume_dist,
        LEAD(cost_cume_dist, 2) OVER (PARTITION BY order_id ORDER BY cost_cume_dist, id) AS next_cost_cume_dist
    FROM with_windows
    WHERE order_id IN (5, 12, 26, 34)
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost_cume_dist DESC, id) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
),
kept_keys AS (
    SELECT order_id
    FROM deduped
    EXCEPT ALL
    SELECT order_id
    FROM shipments
    WHERE order_id = 39
),
verified AS (
    SELECT s.*
    FROM deduped s
    JOIN kept_keys k ON s.order_id = k.order_id
),
joined AS (
    SELECT
        a.*,
        p.discount,
        p.order_date,
        p.status
    FROM verified a
    LEFT JOIN orders p
        ON a.order_id <=> p.id
)
SELECT
    id,
    order_id,
    cost_cume_dist,
    next_cost_cume_dist,
    discount,
    order_date,
    status,
    CASE WHEN status RLIKE '^(SHIPPED|RETURNED)$' THEN 'OTHER_ZONE' WHEN status RLIKE '^(COMPLETED|PENDING)$' THEN 'CORE' ELSE 'PRIORITY' END AS status_zone
FROM joined
ORDER BY discount DESC NULLS LAST;
