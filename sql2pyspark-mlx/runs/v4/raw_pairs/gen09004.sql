WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date,
        CONCAT(status, ' - ', carrier) AS status_carrier
    FROM shipments
    WHERE delivered_date > TO_DATE('2023-08-21')
      AND status IN ('RETURNED', 'LOST')
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        o.order_date,
        o.shipping_fee,
        o.discount
    FROM filtered a
    LEFT JOIN orders o
        ON a.order_id = o.id
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
        days_delivered_date_to_shipped_date,
        status_carrier,
        order_date,
        shipping_fee,
        discount,
        AVG(discount) OVER (ORDER BY order_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS discount_avg_5,
        LAST_VALUE(cost, TRUE) OVER (PARTITION BY order_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost,
        SUM(discount) OVER w_ordered AS running_discount,
        SUM(discount) OVER (ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY status ORDER BY days_delivered_date_to_shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(running_discount) OVER w_grp AS running_discount_avg_6,
        SUM(cumulative_discount) OVER w_ordered AS cumulative_cumulative_discount
    FROM scored
    WINDOW w_grp AS (PARTITION BY carrier ORDER BY discount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_ordered AS (ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount_avg_5 DESC, id) AS dedup_rank
        FROM enriched
    ) ranked
    WHERE dedup_rank = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT status
    FROM deduped
    EXCEPT ALL
    SELECT status
    FROM shipments
    WHERE carrier IN ('UPS', 'FedEx', 'Kerry')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN matched_keys k ON s.status = k.status
),
scored_shipments AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        days_delivered_date_to_shipped_date,
        status_carrier,
        order_date,
        shipping_fee,
        discount,
        discount_avg_5,
        last_known_cost,
        running_discount,
        cumulative_discount,
        running_discount_avg_6,
        cumulative_cumulative_discount,
        dedup_rank,
        MIN(discount) OVER (PARTITION BY carrier) AS carrier_min_discount,
        RANK() OVER (PARTITION BY order_id, status ORDER BY cost DESC NULLS LAST) AS cost_rank
    FROM verified
)
SELECT
    s.id,
    s.order_id,
    s.status,
    s.days_delivered_date_to_shipped_date,
    s.status_carrier,
    s.order_date,
    s.shipping_fee,
    s.last_known_cost,
    s.running_discount,
    (SELECT MIN(t2.unit_price) FROM order_items t2 WHERE t2.order_id = s.order_id) AS min_order_item_unit_price
FROM scored_shipments s
ORDER BY running_discount DESC
