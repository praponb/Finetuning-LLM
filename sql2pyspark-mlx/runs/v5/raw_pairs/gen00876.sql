WITH joined AS (
    -- Attach orders attributes
    SELECT e.*, p.shipping_fee
    FROM shipments e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(shipping_fee) OVER (ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee,
        DENSE_RANK() OVER w_part AS shipping_fee_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY cost NULLS FIRST) AS cost_percent_rank
    FROM joined
    WHERE shipping_fee > 24
    WINDOW w_part AS (PARTITION BY order_id ORDER BY shipping_fee DESC)
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.shipped_date,
        src.delivered_date,
        src.status,
        src.running_shipping_fee,
        b.status AS order_status,
        b.discount
    FROM scored src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, carrier
    FROM with_dims
    EXCEPT
    SELECT id, carrier
    FROM shipments
    WHERE cost >= 76
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.carrier = k.carrier
),
scored_shipments AS (
    -- Ranking and gap features
    SELECT
        shipped_date,
        status,
        order_status,
        SUM(running_shipping_fee) OVER (PARTITION BY carrier) AS carrier_sum_running_shipping_fee,
        STDDEV_SAMP(discount) OVER (PARTITION BY status, carrier) AS status_stddev_samp_discount,
        RANK() OVER (PARTITION BY order_id, order_status ORDER BY running_shipping_fee DESC) AS running_shipping_fee_rank
    FROM kept_rows
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_shipping_fee_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS running_shipping_fee_rank_tier,
    CASE WHEN status_stddev_samp_discount >= 0 THEN 'HIGH' ELSE 'LOW' END AS status_stddev_samp_discount_tier,
    SORT_ARRAY(COLLECT_SET(order_status)) AS order_status_list,
    COUNT_IF(status = 'RETURNED') AS returned_events,
    ROUND(SUM(carrier_sum_running_shipping_fee) / NULLIF(COUNT(CASE WHEN order_status = 'SHIPPED' THEN 1 END), 0), 2) AS carrier_sum_running_shipping_fee_per_shipped,
    SUM(carrier_sum_running_shipping_fee) AS total_carrier_sum_running_shipping_fee
FROM scored_shipments
GROUP BY GROUPING SETS ((CASE WHEN running_shipping_fee_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN status_stddev_samp_discount >= 0 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN running_shipping_fee_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END))
HAVING ROUND(SUM(carrier_sum_running_shipping_fee) / NULLIF(COUNT(CASE WHEN order_status = 'SHIPPED' THEN 1 END), 0), 2) >= 0;
