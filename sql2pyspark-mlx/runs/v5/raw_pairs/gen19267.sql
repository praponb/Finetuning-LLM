WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE last_restock > DATE '2023-09-14'
      AND quantity >= 184
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(quantity, 2) OVER (ORDER BY quantity, id) AS next_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY days_since_last_restock DESC, id) AS row_seq,
        LEAD(days_since_last_restock, 1) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS next_days_since_last_restock
    FROM filtered
),
enriched AS (
    -- Attach stores attributes
    SELECT
        src.*,
        s.store_name,
        s.opened_date
    FROM with_windows src
    LEFT JOIN stores s
        ON src.store_id = s.id
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.*,
        p.city,
        p.store_name AS store_store_name,
        p.opened_date AS store_opened_date
    FROM enriched e
    LEFT JOIN stores p
        ON e.store_id = p.id
),
joined_inventory AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.last_restock,
        s.last_restock_label,
        s.next_quantity,
        s.store_name,
        s.city,
        s.store_opened_date,
        b.city AS store_city,
        b.region,
        b.sqft
    FROM joined s
    LEFT JOIN stores b
        ON s.store_id = b.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last_restock,
        next_quantity,
        city,
        store_city,
        region,
        sqft,
        MAX(next_quantity) OVER w_recent AS last3_max_next_quantity,
        AVG(next_quantity) OVER w_recent AS last3_avg_next_quantity,
        SUM(next_quantity) OVER w_recent AS last3_sum_next_quantity
    FROM joined_inventory
    WINDOW w_recent AS (PARTITION BY region, store_id ORDER BY store_opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY last3_max_next_quantity DESC NULLS LAST, id) = 1
)
SELECT
    id,
    next_quantity,
    store_city,
    region,
    sqft,
    last3_max_next_quantity,
    last3_avg_next_quantity,
    last3_sum_next_quantity
FROM ranked
ORDER BY sqft DESC NULLS LAST, last3_max_next_quantity;
