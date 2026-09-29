WITH cleaned AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        SUBSTRING(city, 1, 4) AS city_prefix
    FROM stores
    WHERE region IS NOT NULL
      AND region IS DISTINCT FROM 'Central'
),
kept_keys AS (
    SELECT city
    FROM cleaned
    INTERSECT ALL
    SELECT city
    FROM stores
    WHERE region NOT IN ('Central', 'North')
),
reconciled AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.city = k.city
),
cleaned_stores AS (
    SELECT *, UPPER(TRIM(store_name)) AS store_name_normalized
    FROM reconciled
    WHERE opened_date > DATE '2023-03-11'
      AND region IS NOT NULL
),
eligible AS (
    SELECT s.*
    FROM cleaned_stores s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock >= s.opened_date
        )
),
filtered AS (
    SELECT
        store_name,
        city,
        region,
        opened_date,
        city_prefix,
        store_name_normalized,
        CONCAT_WS(',', region, city) AS region_city_key
    FROM eligible
    WHERE opened_date > TO_DATE('2023-05-26')
)
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    COUNT(*) FILTER (WHERE region = 'West') AS west_rows,
    COUNT(*) AS event_count,
    COUNT(*) AS row_count,
    COUNT_IF(region = 'East') AS east_events
FROM filtered
GROUP BY year_start;
