WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        DATE_TRUNC('month', last_restock) AS last_restock_period
    FROM inventory
    WHERE last_restock > DATE '2023-06-12'
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.*,
        p.price,
        p.brand,
        p.cost
    FROM base s
    LEFT JOIN products p
        ON s.product_id = p.id
    WHERE p.launch_date < '2024-10-01'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        price,
        brand,
        UPPER(brand) AS brand_upper
    FROM enriched
    WHERE last_restock >= DATE '2023-05-21'
      AND brand IN ('Initech', 'Umbrella', 'Globex')
),
base_inventory AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        quantity,
        last_restock,
        price,
        brand_upper,
        FLOOR(price) AS price_whole
    FROM valid
    WHERE last_restock >= TO_DATE('2023-06-21')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        quantity,
        last_restock,
        price,
        brand_upper,
        price_whole,
        FLOOR(quantity) AS quantity_whole,
        price_whole + quantity AS price_whole_plus_quantity
    FROM base_inventory
    WHERE last_restock >= DATE '2023-06-18'
),
enriched_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        price_whole,
        price_whole_plus_quantity,
        MAX(quantity) OVER w_running AS running_max_quantity,
        SUM(quantity) OVER w_running AS running_sum_quantity,
        MIN(quantity) OVER w_running AS running_min_quantity,
        ROW_NUMBER() OVER (ORDER BY quantity_whole ASC, id ASC) AS row_seq
    FROM cleaned
    WINDOW w_running AS (ORDER BY price_whole_plus_quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    price_whole_plus_quantity,
    running_sum_quantity,
    running_min_quantity,
    row_seq
FROM enriched_inventory
ORDER BY price_whole_plus_quantity DESC, running_sum_quantity DESC
