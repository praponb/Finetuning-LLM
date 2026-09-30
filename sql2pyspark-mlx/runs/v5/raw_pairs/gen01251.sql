WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        HOUR(started_at) started_at_hour,
        UPPER(browser) browser_upper
    FROM sessions
    WHERE country IS DISTINCT FROM 'DE'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id, id
    FROM valid
    INTERSECT ALL
    SELECT customer_id, id
    FROM sessions
    WHERE device = 'tablet'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.device,
        a.country,
        a.started_at_hour,
        b.city
    FROM verified a
    INNER JOIN customers b
        ON a.customer_id <=> b.id
    WHERE b.country = 'SG'
),
scored AS (
    -- Ranking and gap features
    SELECT *, DENSE_RANK() OVER (ORDER BY started_at_hour) started_at_hour_dense_rank
    FROM enriched
)
-- Grouped metrics
SELECT
    country,
    COUNT_IF(device = 'mobile') AS mobile_events,
    ROUND(AVG(started_at_hour_dense_rank), 2) AS avg_started_at_hour_dense_rank,
    COUNT(*) FILTER (WHERE country = 'US') AS us_rows,
    MAX(started_at_hour_dense_rank) FILTER (WHERE city = 'Tokyo') AS tokyo_max_started_at_hour_dense_rank
FROM scored
GROUP BY country
ORDER BY country DESC;
