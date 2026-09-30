WITH kept_keys AS (
    -- Set difference on keys
    SELECT status, order_id
    FROM shipments
    EXCEPT ALL
    SELECT status, order_id
    FROM shipments
    WHERE cost BETWEEN 77 AND 82
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM shipments s
    JOIN kept_keys k ON s.status = k.status AND s.order_id = k.order_id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_ADD(shipped_date, 72) AS shipped_date_plus_72d
    FROM kept_rows
    WHERE delivered_date > DATE '2023-04-17'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share,
        DENSE_RANK() OVER (PARTITION BY carrier ORDER BY cost NULLS FIRST) AS cost_dense_rank
    FROM filtered
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN status IN ('IN_TRANSIT', 'RETURNED') THEN 'Group A' ELSE 'Other' END AS status_group
    FROM enriched
    WHERE shipped_date > DATE '2023-02-27'
),
joined AS (
    -- Attach orders attributes
    SELECT s.*, b.amount
    FROM base s
    JOIN orders b
        ON s.order_id = b.id
    WHERE b.channel <> 'partner'
),
filtered_shipments AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(carrier) AS carrier_lower,
        COALESCE(carrier, 'UNKNOWN') AS resolved_carrier
    FROM joined
    WHERE shipped_date > TO_DATE('2023-04-24')
      AND order_id IN (17, 32)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        carrier,
        cost_share,
        amount,
        cost
    FROM filtered_shipments
) src
UNPIVOT (
    val FOR measure IN (cost_share, amount, cost)
)
