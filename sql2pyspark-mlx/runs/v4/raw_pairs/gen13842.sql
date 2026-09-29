WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, value
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(attr_key) AS attr_key_upper,
        UPPER(attr_key) AS attr_key_upper_2,
        COALESCE(attr_key, 'UNKNOWN') AS attr_key_clean
    FROM exploded
    WHERE created_at >= DATE '2025-03-01'
      AND attr_key IS NOT NULL
),
base_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TO_DATE(created_at) AS created_at_date,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at,
        COALESCE(attr_key, 'UNKNOWN') AS resolved_attr_key
    FROM base
    WHERE created_at >= TIMESTAMP '2025-03-02 12:00:00'
      AND attr_key IS NOT NULL
)
SELECT
    id,
    attr_key_upper_2,
    attr_key_clean,
    created_at_date
FROM base_carts;
