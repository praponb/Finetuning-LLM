WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS LAST, id) AS row_num
    FROM web_events
    QUALIFY row_num = 1
),
joined AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        p.device,
        p.country,
        p.browser
    FROM latest a
    LEFT JOIN sessions p
        ON a.session_id = p.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        device,
        country,
        browser,
        LENGTH(device) AS device_length
    FROM joined
    WHERE country IS NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        session_id,
        device_length
    FROM base
    QUALIFY RANK() OVER (PARTITION BY browser, device ORDER BY device_length DESC) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER w_part AS device_length_bucket,
        SUM(device_length) OVER (ORDER BY device_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_device_length,
        LEAD(device_length, 1) OVER w_part AS next_device_length,
        ROW_NUMBER() OVER w_part AS row_seq,
        NTILE(3) OVER w_hist AS device_length_bucket_2
    FROM deduped
    WHERE device_length <= 29
    WINDOW w_part AS (PARTITION BY session_id ORDER BY device_length, id), w_hist AS (ORDER BY device_length DESC, id)
)
SELECT
    id,
    session_id,
    cumulative_device_length,
    row_seq,
    device_length_bucket_2
FROM with_windows
ORDER BY id NULLS LAST
