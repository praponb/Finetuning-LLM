WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END AS is_bangkok,
        TRIM(store_name) AS clean_store_name
    FROM stores
    WHERE region IS NOT NULL
      AND city = 'Berlin'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC NULLS FIRST, id) AS dedup_rank
        FROM valid
    ) ranked
    WHERE dedup_rank = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date BETWEEN '2023-11-01' AND '2024-05-01'
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM eligible
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY is_bangkok DESC, id) = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date, id) AS latest_rank
        FROM latest
    ) ranked
    WHERE latest_rank = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT city, region
    FROM deduped
    EXCEPT DISTINCT
    SELECT city, region
    FROM stores
    WHERE store_name NOT LIKE 'D%'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN kept_keys k ON s.city = k.city AND s.region = k.region
)
SELECT
    id,
    store_name,
    city,
    region,
    is_bangkok,
    clean_store_name,
    dedup_rank,
    latest_rank
FROM verified
ORDER BY id NULLS LAST;
