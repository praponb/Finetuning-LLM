WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, value
),
remaining_keys AS (
    SELECT id
    FROM lines
    EXCEPT ALL
    SELECT id
    FROM carts
    WHERE HOUR(created_at) BETWEEN 6 AND 9
),
verified AS (
    SELECT s.*
    FROM lines s
    INNER JOIN remaining_keys k ON s.id = k.id
),
joined AS (
    SELECT
        src.*,
        d.country,
        d.name
    FROM verified src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
base AS (
    SELECT
        id,
        customer_id,
        created_at,
        k,
        value,
        name,
        COALESCE(k, 'N/A') AS k_filled,
        REPLACE(name, ' ', '') AS name_slug,
        COALESCE(k, 'N/A') AS k_clean
    FROM joined
    WHERE k = 'weight_kg'
),
valid AS (
    SELECT
        *,
        UPPER(TRIM(name)) AS name_normalized,
        CONCAT(k, '/', name) AS k_name
    FROM base
    WHERE k IS NOT NULL
      AND LENGTH(name) < 11
)
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    k,
    COUNT(CASE WHEN k = 'device' THEN 1 END) AS device_count,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN k = 'weight_kg' THEN 1 END) AS weight_kg_count,
    COUNT(*) AS event_count_2,
    COUNT(CASE WHEN k = 'weight_kg' THEN 1 END) AS weight_kg_count_2
FROM valid
GROUP BY GROUPING SETS ((DATE_TRUNC('DAY', created_at), k), (DATE_TRUNC('DAY', created_at)), ())
ORDER BY event_count_2 DESC
