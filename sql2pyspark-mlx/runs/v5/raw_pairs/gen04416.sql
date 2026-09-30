WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(supplier_name) AS clean_supplier_name
    FROM suppliers
    WHERE city IS DISTINCT FROM 'Chiang Mai'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS event_seq,
        AVG(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rating_avg_3_prior,
        MAX(rating) OVER w_hist AS city_max_rating
    FROM base
    WINDOW w_hist AS (PARTITION BY city)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        country,
        rating,
        clean_supplier_name,
        event_seq,
        rating_avg_3_prior,
        NTILE(4) OVER (PARTITION BY city, country ORDER BY rating_avg_3_prior DESC, id) AS rating_avg_3_prior_bucket,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY country), 4) AS event_seq_share
    FROM metrics
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        rating,
        event_seq,
        rating_avg_3_prior_bucket,
        event_seq_share,
        CEIL(event_seq_share) AS event_seq_share_whole
    FROM enriched
    WHERE rating_avg_3_prior_bucket != 3
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.price < 930
        )
)
-- Grouped metrics
SELECT
    country,
    SUM(CASE WHEN country = 'JP' THEN rating_avg_3_prior_bucket ELSE 0 END) AS jp_rating_avg_3_prior_bucket,
    MIN(event_seq) AS min_event_seq,
    AVG(rating) AS avg_rating
FROM not_flagged
GROUP BY CUBE (country);
