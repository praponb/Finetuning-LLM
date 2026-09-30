WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        CONCAT(region, ' ', city) AS region_city
    FROM stores
    WHERE opened_date > DATE '2023-07-01'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        sqft,
        region_city,
        CASE WHEN city = 'New York' THEN 1 ELSE 0 END AS is_new_york
    FROM valid
    WHERE city != 'Singapore'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY city ORDER BY is_new_york, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sqft_avg_6,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft
    FROM cleaned
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.sqft,
        s.is_new_york,
        s.sqft_avg_6,
        s.first_known_sqft,
        latest.inventory_last_restock,
        latest.quantity
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date <= s.inventory_last_restock
        )
),
with_windows_stores AS (
    -- Ranking and gap features
    SELECT
        id,
        sqft,
        inventory_last_restock,
        quantity,
        LAG(sqft) OVER w_running AS prev_sqft,
        LEAD(sqft) OVER w_running AS next_sqft,
        LAG(first_known_sqft) OVER w_running AS prev_first_known_sqft,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM not_flagged
    WINDOW w_running AS (ORDER BY sqft, id)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.store_id = 18
        )
)
SELECT
    id,
    sqft,
    inventory_last_restock,
    quantity,
    prev_sqft,
    prev_first_known_sqft
FROM eligible
