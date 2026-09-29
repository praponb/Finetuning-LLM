WITH filtered AS (
    SELECT *, LENGTH(region) AS region_length
    FROM stores
    WHERE opened_date >= '2024-10-01'
),
kept AS (
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock < s.opened_date
        )
),
deduped AS (
    SELECT *
    FROM kept
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date, id) = 1
),
kept_keys AS (
    SELECT region, id
    FROM deduped
    EXCEPT DISTINCT
    SELECT region, id
    FROM stores
    WHERE city = 'New York'
),
reconciled AS (
    SELECT s.*
    FROM deduped s
    INNER JOIN kept_keys k ON s.region = k.region AND s.id = k.id
)
SELECT
    store_name,
    city,
    region,
    region_length
FROM reconciled
ORDER BY region DESC NULLS LAST;
