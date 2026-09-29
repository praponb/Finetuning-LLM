WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        status,
        SUBSTRING(carrier, 1, 3) AS carrier_prefix,
        CONCAT(status, ', ', carrier) AS status_carrier
    FROM shipments
    WHERE carrier IS DISTINCT FROM 'Kerry'
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.status AS order_status,
        p.channel
    FROM valid s
    LEFT JOIN orders p
        ON s.order_id = p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status,
        carrier_prefix,
        channel,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY delivered_date, id) AS row_seq,
        ROW_NUMBER() OVER w_part AS event_seq,
        FIRST_VALUE(order_status) IGNORE NULLS OVER w_hist AS first_known_order_status
    FROM enriched
    WINDOW w_part AS (ORDER BY delivered_date, id), w_hist AS (ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY event_seq DESC) AS row_num
        FROM with_windows
    ) t
    WHERE row_num = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(row_seq) OVER w_grp AS prev_row_seq,
        LEAD(row_seq) OVER w_grp AS next_row_seq,
        LEAD(seq_no) OVER w_grp AS next_seq_no,
        ROW_NUMBER() OVER w_grp AS event_seq_2,
        FIRST_VALUE(event_seq, TRUE) OVER (PARTITION BY status ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_seq
    FROM deduped
    WINDOW w_grp AS (PARTITION BY channel ORDER BY event_seq, id)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, status ORDER BY event_seq, id) AS dedup_rank
    FROM metrics
    QUALIFY dedup_rank = 1
),
latest_shipments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY row_num DESC, id) AS rn
    FROM latest
    QUALIFY rn = 1
)
SELECT
    id,
    status,
    carrier_prefix,
    channel,
    row_seq,
    event_seq,
    row_num,
    prev_row_seq,
    next_row_seq
FROM latest_shipments
ORDER BY event_seq DESC
