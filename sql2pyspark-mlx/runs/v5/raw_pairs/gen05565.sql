WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        order_date,
        status,
        channel,
        amount,
        COALESCE(amount, 0) AS amount_filled,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM orders
    WHERE customer_id IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM valid
    EXCEPT DISTINCT
    SELECT customer_id, id
    FROM orders
    WHERE customer_id = 22
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.carrier IN ('UPS', 'Kerry')
        )
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM eligible s
    INNER JOIN promotions pr
        ON s.channel = pr.channel
       AND s.order_date BETWEEN pr.starts_on AND pr.ends_on
)
SELECT
    id,
    order_date,
    status,
    channel,
    amount,
    amount_filled,
    channel_filled,
    promo_id,
    promo_code,
    CASE WHEN status RLIKE '^(CANCELLED|COMPLETED)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS status_zone
FROM attributed
ORDER BY channel NULLS FIRST;
