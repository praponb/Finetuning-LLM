WITH flattened AS (
    SELECT
        id,
        customer_id,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS key, attr_value
),
joined AS (
    SELECT
        s.id,
        s.customer_id,
        s.key,
        s.attr_value,
        d.country
    FROM flattened s
    INNER JOIN customers d
        ON s.customer_id = d.id
    WHERE LENGTH(d.name) >= 8
),
cleaned AS (
    SELECT
        id,
        customer_id,
        key,
        attr_value,
        country,
        SUBSTRING(country, 1, 3) AS country_prefix
    FROM joined
    WHERE country = 'JP'
),
valid AS (
    SELECT
        id,
        key,
        country,
        COALESCE(country, 'unassigned') AS country_filled,
        COALESCE(country, 'UNKNOWN') AS country_clean
    FROM cleaned
    WHERE customer_id IS NOT NULL
),
base AS (
    SELECT
        id,
        key,
        country_filled,
        country_clean,
        CONCAT(country, '/', key) AS country_key,
        COALESCE(key, 'OTHER') AS resolved_key
    FROM valid
    WHERE country IS DISTINCT FROM 'SG'
),
totals AS (
    SELECT
        key,
        SIZE(COLLECT_SET(key)) AS num_keys,
        COUNT(*) FILTER (WHERE key = 'gift') AS gift_rows,
        SIZE(COLLECT_SET(key)) AS num_keys_2,
        GROUPING_ID() AS grouping_id
    FROM base
    GROUP BY ROLLUP (key)
    HAVING SIZE(COLLECT_SET(key)) > 1
)
SELECT
    key,
    num_keys,
    gift_rows,
    num_keys_2
FROM totals
ORDER BY num_keys_2 DESC;
