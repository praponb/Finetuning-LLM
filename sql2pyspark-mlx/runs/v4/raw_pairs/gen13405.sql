WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(carrier) AS clean_carrier,
        COALESCE(carrier, 'N/A') AS carrier_filled
    FROM shipments
    WHERE shipped_date >= DATE '2024-03-07'
      AND carrier IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        clean_carrier,
        carrier_filled,
        LAST_VALUE(cost, TRUE) OVER w_recent AS last_known_cost,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC, id) AS seq_no
    FROM valid
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid_shipments AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(status) AS status_length
    FROM enriched
    WHERE delivered_date > DATE '2023-04-05'
      AND order_id IS NOT NULL
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.amount,
        d.channel
    FROM valid_shipments a
    LEFT JOIN orders d
        ON a.order_id = d.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched_shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date NULLS FIRST, id) = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY delivered_date DESC NULLS LAST) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(status_length, 0) AS status_length_rounded,
        FLOOR(seq_no) AS seq_no_whole,
        COALESCE(carrier, 'OTHER') AS carrier_filled_2
    FROM deduped
    WHERE status_length <= 26
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        carrier,
        last_known_cost
    FROM base
) src
PIVOT (
    MAX(last_known_cost)
    FOR carrier IN ('Kerry' AS kerry, 'Flash' AS flash, 'FedEx' AS fedex)
)
