WITH base AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        INITCAP(TRIM(carrier)) AS carrier_title,
        COALESCE(carrier, 'N/A') AS resolved_carrier
    FROM shipments
    WHERE delivered_date >= TO_DATE('2023-03-05')
      AND delivered_date >= '2024-01-23'
),
matched_keys AS (
    SELECT status, order_id
    FROM base
    EXCEPT DISTINCT
    SELECT status, order_id
    FROM shipments
    WHERE status <> 'RETURNED'
),
reconciled AS (
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.status = k.status AND s.order_id = k.order_id
),
metrics AS (
    SELECT
        *,
        RANK() OVER (ORDER BY cost ASC NULLS LAST) AS cost_rank,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share
    FROM reconciled
),
filtered AS (
    SELECT *, SUBSTRING(carrier, 1, 4) AS carrier_prefix
    FROM metrics
    WHERE delivered_date > DATE '2023-07-13'
      AND carrier IS NOT NULL
),
matched_keys_2 AS (
    SELECT status, order_id
    FROM filtered
    EXCEPT
    SELECT status, order_id
    FROM shipments
    WHERE shipped_date BETWEEN '2024-02-19' AND '2025-03-01'
),
kept_rows AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys_2 k ON s.status = k.status AND s.order_id = k.order_id
)
SELECT
    s.id,
    s.order_id,
    s.delivered_date,
    s.cost,
    s.status,
    s.carrier_title,
    s.cost_rank,
    s.cost_share,
    (SELECT SUM(o2.quantity) FROM order_items o2 WHERE o2.order_id = s.order_id) AS sum_order_item_quantity
FROM kept_rows s
ORDER BY id ASC NULLS FIRST;
