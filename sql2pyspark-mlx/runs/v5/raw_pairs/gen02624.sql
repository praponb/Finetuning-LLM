WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        city,
        rating
    FROM suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating NULLS LAST, id) = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM latest
    INTERSECT
    SELECT id, city
    FROM suppliers
    WHERE supplier_name LIKE 'G%'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN remaining_keys k ON s.id = k.id AND s.city = k.city
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.brand IN ('Globex', 'Initech', 'Acme')
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND SIZE(c.tags) > 2
        )
),
kept_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.price > 1070
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND LENGTH(ch.product_name) > 12
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY city) AS city_sum_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        DENSE_RANK() OVER (ORDER BY rating DESC) AS rating_dense_rank,
        RANK() OVER (PARTITION BY city ORDER BY rating DESC NULLS FIRST) AS rating_rank,
        STDDEV_SAMP(rating) OVER (PARTITION BY city) AS city_stddev_samp_rating
    FROM not_flagged
)
-- Multi-level aggregation
SELECT
    city,
    COUNT(*) AS event_count,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT_IF(city = 'London') AS london_events,
    ROUND(AVG(rating_rank), 2) AS avg_rating_rank,
    ROUND(SUM(rating_share) / NULLIF(COUNT(CASE WHEN city = 'Bangkok' THEN 1 END), 0), 2) AS rating_share_per_bangkok,
    ROUND(SUM(rating_rank) / NULLIF(COUNT(CASE WHEN city = 'Berlin' THEN 1 END), 0), 2) AS rating_rank_per_berlin
FROM enriched
GROUP BY 1;
