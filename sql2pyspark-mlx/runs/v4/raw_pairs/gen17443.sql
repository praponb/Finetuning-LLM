WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        region,
        INITCAP(TRIM(city)) AS city_title
    FROM stores
    WHERE YEAR(opened_date) = 2023
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(store_name) AS store_name_length
    FROM filtered
    WHERE region IS NOT NULL
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock >= '2023-03-18'
        )
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY store_name_length DESC, id) AS dedup_rank
        FROM not_flagged
    ) t
    WHERE dedup_rank = 1
)
SELECT
    id,
    region,
    city_title,
    store_name_length,
    dedup_rank
FROM deduped
ORDER BY region DESC NULLS LAST;
