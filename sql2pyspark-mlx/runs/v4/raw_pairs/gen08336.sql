WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('month', opened_date) AS opened_date_period
    FROM stores
    WHERE region IS NOT NULL
      AND city IN ('Chiang Mai', 'Tokyo', 'Berlin')
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, region
    FROM filtered
    EXCEPT
    SELECT id, region
    FROM stores
    WHERE region IN ('North', 'South')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN matched_keys k ON s.id = k.id AND s.region = k.region
),
matched_keys_2 AS (
    -- Set intersection on keys
    SELECT region, id
    FROM kept_rows
    INTERSECT
    SELECT region, id
    FROM stores
    WHERE YEAR(opened_date) = 2024
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    JOIN matched_keys_2 k ON s.region = k.region AND s.id = k.id
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date <= s.opened_date_period
        )
)
SELECT
    id,
    city,
    region
FROM not_flagged;
