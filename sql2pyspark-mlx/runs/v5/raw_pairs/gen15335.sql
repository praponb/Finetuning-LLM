WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        CASE WHEN status IN ('LOST', 'DELIVERED') THEN 'Group A' ELSE 'Other' END AS status_group,
        ADD_MONTHS(shipped_date, 1) AS shipped_date_plus_months,
        COALESCE(carrier, 'NONE') AS carrier_filled
    FROM shipments
    WHERE order_id IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        carrier,
        status,
        status_group,
        shipped_date_plus_months
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY shipped_date DESC, id) = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        carrier,
        status_group,
        shipped_date_plus_months
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date_plus_months NULLS FIRST, id) AS latest_rank
        FROM latest
    ) x
    WHERE latest_rank = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        carrier,
        shipped_date_plus_months
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date_plus_months DESC NULLS FIRST, id) AS rank_in_group
        FROM deduped
    ) x
    WHERE rank_in_group = 1
)
SELECT
    id,
    carrier,
    shipped_date_plus_months,
    CASE WHEN carrier RLIKE '^(DHL|Flash)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS carrier_zone
FROM ranked
