WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock < s.opened_date
        )
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    COUNT(*) FILTER (WHERE city = 'London') AS london_rows,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SIZE(COLLECT_SET(city)) AS num_cities_2
FROM kept
GROUP BY year_start
