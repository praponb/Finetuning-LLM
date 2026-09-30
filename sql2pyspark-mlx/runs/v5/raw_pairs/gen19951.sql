WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        HOUR(paid_at) AS paid_at_hour,
        SUBSTRING(status, 1, 3) AS status_prefix,
        COALESCE(method, 'DIRECT') AS method_filled
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-11-12 17:00:00'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY status, method ORDER BY paid_at_hour DESC, id ASC) AS paid_at_hour_bucket,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at_hour ASC, id ASC) AS event_seq
    FROM valid
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        paid_at_hour,
        status_prefix,
        method_filled,
        paid_at_hour_bucket,
        event_seq,
        TO_DATE(paid_at) AS paid_at_date,
        COALESCE(method, 'UNKNOWN') AS method_clean
    FROM scored
    WHERE paid_at_hour IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY event_seq DESC NULLS FIRST, id ASC) = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        amount,
        status,
        status_prefix,
        paid_at_hour_bucket,
        event_seq,
        paid_at_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) AS rn
        FROM ranked
    ) t
    WHERE rn = 1
)
SELECT
    id,
    amount,
    status,
    status_prefix,
    paid_at_hour_bucket,
    event_seq,
    CASE WHEN status RLIKE '^(SUCCESS|FAILED)$' THEN 'EXTENDED' ELSE 'CORE' END AS status_zone
FROM deduped
ORDER BY amount ASC NULLS FIRST
