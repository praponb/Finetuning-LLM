WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        LAST_DAY(delivered_date) delivered_date_month_end,
        SUBSTRING(status, 1, 4) status_prefix
    FROM shipments
    WHERE carrier IS NOT NULL
      AND status IN ('LOST', 'IN_TRANSIT', 'RETURNED')
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        status,
        delivered_date_month_end,
        status_prefix
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date, id) = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date_month_end NULLS FIRST, id) AS dedup_rank
        FROM deduped
    ) ranked
    WHERE dedup_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY dedup_rank DESC, id) seq_no,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY dedup_rank, id) event_seq,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date_month_end, id) row_seq,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY dedup_rank, id) row_seq_2,
        ROW_NUMBER() OVER (ORDER BY dedup_rank DESC, id) seq_no_2
    FROM latest
)
SELECT
    id,
    status,
    delivered_date_month_end,
    status_prefix,
    event_seq,
    row_seq,
    row_seq_2,
    seq_no_2,
    CASE WHEN status RLIKE '^(RETURNED|DELIVERED)$' THEN 'GROUP_B' WHEN status RLIKE '^(IN_TRANSIT|LOST)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS status_zone
FROM metrics
ORDER BY status_zone DESC NULLS FIRST
