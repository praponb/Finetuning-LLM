WITH eligible AS (
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock < '2024-08-21'
        )
),
not_flagged AS (
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock < s.opened_date
        )
),
enriched AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY city ORDER BY sqft ASC NULLS FIRST) sqft_cume_dist,
        LEAD(sqft, 2, 0) OVER (PARTITION BY region ORDER BY opened_date ASC, id ASC) next_sqft
    FROM not_flagged
)
SELECT
    id,
    store_name,
    city,
    opened_date,
    sqft,
    next_sqft
FROM enriched
ORDER BY city ASC NULLS LAST;
