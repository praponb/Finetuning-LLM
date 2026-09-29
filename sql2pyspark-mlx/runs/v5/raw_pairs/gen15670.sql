WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        launch_date,
        price - cost AS margin,
        COALESCE(brand, 'DIRECT') AS resolved_brand
    FROM products
    WHERE launch_date >= TO_DATE('2023-07-19')
      AND brand IS NOT NULL
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.brand,
        s.price,
        s.cost,
        s.launch_date,
        latest.last_restock
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        brand,
        price,
        cost,
        launch_date,
        last_restock,
        LEAD(cost) OVER (PARTITION BY brand ORDER BY cost, id) AS next_cost,
        LEAD(price) OVER (PARTITION BY brand ORDER BY cost, id) AS next_price,
        LAG(price) OVER (PARTITION BY brand ORDER BY cost, id) AS prev_price,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost, id) AS seq_no
    FROM with_last
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY prev_price DESC NULLS LAST, id) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT brand
    FROM deduped
    INTERSECT ALL
    SELECT brand
    FROM products
    WHERE UPPER(TRIM(product_name)) = 'COFFEE MAKER'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN matched_keys k ON s.brand = k.brand
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        brand,
        price,
        cost,
        launch_date,
        last_restock,
        next_cost,
        next_price,
        prev_price,
        seq_no,
        latest_rank,
        ROUND(seq_no * 1.1, 2) AS seq_no_scaled,
        COALESCE(brand, 'OTHER') AS resolved_brand
    FROM kept_rows
    WHERE prev_price > 350
)
-- Aggregate with conditional metrics
SELECT
    brand,
    CASE WHEN price >= 930 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
    STDDEV_SAMP(cost) AS cost_stddev,
    COUNT_IF(brand = 'Initech') AS initech_events,
    APPROX_PERCENTILE(next_price, 0.95) AS next_price_p95
FROM cleaned
GROUP BY 1, 2
HAVING COUNT_IF(brand = 'Initech') > 1;
