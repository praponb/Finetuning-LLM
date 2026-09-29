WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        country,
        city,
        rating,
        CONCAT(city, '/', supplier_name) AS city_supplier_name,
        COALESCE(city, 'N/A') AS resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand IN ('Initech', 'Acme', 'Globex')
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        city,
        rating,
        city_supplier_name,
        resolved_city,
        LEAD(rating, 1) OVER (ORDER BY rating, id) AS next_rating,
        ROW_NUMBER() OVER w_running AS event_seq,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country, city), 4) AS rating_share
    FROM kept
    WINDOW w_running AS (PARTITION BY city ORDER BY rating, id)
),
metrics_suppliers AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(event_seq, 1, 0) OVER (PARTITION BY city ORDER BY next_rating, id) AS prev_event_seq,
        SUM(event_seq) OVER (PARTITION BY country, city ORDER BY next_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_seq,
        FIRST_VALUE(event_seq, TRUE) OVER (PARTITION BY city ORDER BY next_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_seq,
        NTILE(5) OVER w_running AS event_seq_bucket
    FROM metrics
    WINDOW w_running AS (PARTITION BY city ORDER BY event_seq DESC, id)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY first_known_event_seq DESC, id) AS dedup_rank
    FROM metrics_suppliers
    QUALIFY dedup_rank = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        country,
        rating
    FROM latest
) src
PIVOT (
    AVG(rating) AS total,
    COUNT(1) AS cnt
    FOR country IN ('TH' AS th, 'US' AS us, 'SG' AS sg)
)
