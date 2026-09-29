WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        REPLACE(store_name, ' ', '') AS store_name_slug
    FROM stores
    WHERE opened_date >= DATE '2023-09-20'
      AND region IS NOT DISTINCT FROM 'Central'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        store_name_slug,
        LAST_VALUE(sqft, TRUE) OVER (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        FIRST_VALUE(sqft) IGNORE NULLS OVER w_recent AS first_known_sqft
    FROM valid
    WINDOW w_recent AS (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_stores AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(sqft_share) OVER w_recent AS next_sqft_share,
        LEAD(last_known_sqft) OVER w_recent AS next_last_known_sqft,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM scored
    WINDOW w_recent AS (PARTITION BY region, city ORDER BY opened_date, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.discount BETWEEN 0.14 AND 0.39
        )
),
valid_stores AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        region,
        opened_date,
        store_name_slug,
        last_known_sqft,
        sqft_share,
        first_known_sqft,
        next_sqft_share,
        event_seq,
        ROUND(event_seq / sqft, 2) AS event_seq_per_sqft
    FROM not_flagged
    WHERE region IS NOT NULL
      AND opened_date > DATE '2023-08-07'
)
SELECT
    id,
    region,
    store_name_slug,
    last_known_sqft,
    first_known_sqft,
    next_sqft_share,
    event_seq,
    event_seq_per_sqft
FROM valid_stores
ORDER BY first_known_sqft;
