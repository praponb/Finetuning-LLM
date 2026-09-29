WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        revenue,
        LEAD(units, 1, 0) OVER (PARTITION BY product_id ORDER BY revenue, id) AS next_units,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date, id) AS row_seq,
        SUM(revenue) OVER w_ordered AS running_revenue
    FROM store_sales
    WINDOW w_ordered AS (PARTITION BY product_id, store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.next_units,
        e.running_revenue,
        p.city
    FROM scored e
    LEFT JOIN stores p
        ON e.store_id = p.id
    WHERE p.sqft >= 4976
),
enriched_store_sales AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.running_revenue,
        s.city,
        p.opened_date,
        p.store_name
    FROM enriched s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
enriched_store_sales_2 AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.city,
        s.opened_date,
        p2.city AS store_city,
        p2.region
    FROM enriched_store_sales s
    JOIN stores p2
        ON s.store_id = p2.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        store_city
    FROM enriched_store_sales_2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        p.sqft,
        p.store_name,
        p.city
    FROM ranked e
    LEFT JOIN stores p
        ON e.store_id = p.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        store_name
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sqft, id) = 1
)
SELECT
    s.id,
    s.store_id,
    (SELECT MIN(t2.quantity) FROM inventory t2 WHERE t2.store_id = s.store_id) AS min_inventory_quantity
FROM latest s;
