WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        elem.product_id,
        elem.qty,
        elem.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) e AS elem
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(product_id, 2) AS product_id_rounded
    FROM lines
    WHERE created_at >= TIMESTAMP '2025-03-01 23:00:00'
      AND qty BETWEEN 2 AND 4
),
base AS (
    -- Keep recent, valid rows
    SELECT *, DAY(created_at) AS created_at_day
    FROM filtered
    WHERE created_at > TIMESTAMP '2025-03-01 16:00:00'
      AND product_id > 6
),
base_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(promo) AS promo_upper,
        COALESCE(promo, 'UNKNOWN') AS promo_filled
    FROM base
    WHERE created_at >= TIMESTAMP '2025-03-01 21:00:00'
      AND promo IS NOT DISTINCT FROM 'WELCOME5'
),
base_carts_2 AS (
    -- Keep recent, valid rows
    SELECT
        *,
        product_id + created_at_day AS product_id_plus_created_at_day,
        COALESCE(promo, 'UNKNOWN') AS resolved_promo
    FROM base_carts
    WHERE created_at >= TIMESTAMP '2025-03-01 15:00:00'
      AND promo IS NOT NULL
),
matched_keys AS (
    -- Set intersection on keys
    SELECT customer_id, id
    FROM base_carts_2
    INTERSECT
    SELECT customer_id, id
    FROM carts
    WHERE customer_id = 21
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base_carts_2 s
    JOIN matched_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
base_carts_3 AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        promo,
        created_at_day,
        promo_upper,
        promo_filled,
        product_id_plus_created_at_day,
        COALESCE(promo, 'unassigned') AS promo_filled_2
    FROM kept_rows
    WHERE created_at >= TIMESTAMP '2025-03-01 19:00:00'
)
SELECT
    customer_id,
    promo,
    created_at_day,
    promo_upper,
    promo_filled,
    product_id_plus_created_at_day,
    CASE WHEN promo RLIKE '^(SPRING10|VIP)$' THEN 'PRIORITY' WHEN promo RLIKE '^(BUNDLE|WELCOME5)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS promo_zone
FROM base_carts_3
ORDER BY product_id_plus_created_at_day;
