WITH exploded AS (
    SELECT
        id,
        created_at,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS key, attr_value
),
scored AS (
    SELECT
        id,
        created_at,
        key,
        attr_value,
        FIRST_VALUE(key) IGNORE NULLS OVER w_running AS first_known_key,
        LAST_VALUE(key, TRUE) OVER w_running AS last_known_key
    FROM exploded
    WINDOW w_running AS (PARTITION BY key ORDER BY created_at, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY created_at DESC, id, key) = 1
),
matched_keys AS (
    SELECT id
    FROM latest
    EXCEPT
    SELECT id
    FROM carts
    WHERE SIZE(coupon_codes) >= 1
),
kept_rows AS (
    SELECT s.*
    FROM latest s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    key,
    attr_value,
    first_known_key,
    CASE WHEN key RLIKE '^(weight_kg|device)$' THEN 'CORE' WHEN key RLIKE '^(utm_source|gift)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS key_zone
FROM kept_rows
ORDER BY id DESC;
