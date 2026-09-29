WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS key, attr_value
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY key, customer_id ORDER BY created_at, id, key) AS seq_no,
        FIRST_VALUE(key, TRUE) OVER w_part AS first_known_key
    FROM lines
    WHERE key IN ('utm_source', 'device', 'gift')
    WINDOW w_part AS (ORDER BY created_at, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.email,
        d.country
    FROM scored src
    JOIN customers d
        ON src.customer_id = d.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(email) AS email_length,
        COALESCE(key, 'DIRECT') AS key_filled
    FROM enriched
    WHERE created_at >= TIMESTAMP '2025-03-01 17:00:00'
      AND HOUR(created_at) BETWEEN 10 AND 18
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        key,
        seq_no
    FROM cleaned
) src
PIVOT (
    SUM(seq_no) AS amount,
    COUNT(1) AS cnt
    FOR key IN ('utm_source', 'gift')
)
