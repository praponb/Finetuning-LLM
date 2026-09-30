WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(quantity AS DOUBLE) AS quantity_num
    FROM inventory
    WHERE last_restock > DATE '2023-09-25'
      AND quantity != 83
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_id,
        last_restock,
        quantity_num
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity DESC, id) AS dedup_rank
        FROM valid
    ) ranked
    WHERE dedup_rank = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.region,
        b.opened_date,
        b.city
    FROM ranked e
    JOIN stores b
        ON e.store_id = b.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ADD_MONTHS(opened_date, 6) AS opened_date_plus_months,
        ROUND(quantity_num * 1.1, 2) AS adjusted_quantity_num
    FROM joined
    WHERE last_restock > TO_DATE('2023-08-15')
      AND region IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY city ORDER BY quantity_num DESC, id) AS quantity_num_bucket,
        LEAD(adjusted_quantity_num, 1) OVER w_running AS next_adjusted_quantity_num,
        LAG(adjusted_quantity_num, 2) OVER (PARTITION BY region, store_id ORDER BY adjusted_quantity_num, id) AS prev_adjusted_quantity_num
    FROM filtered
    WINDOW w_running AS (PARTITION BY region, city ORDER BY opened_date, id)
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.last_restock,
        src.region,
        src.opened_date,
        src.city,
        src.quantity_num_bucket,
        src.next_adjusted_quantity_num,
        b.city AS store_city
    FROM with_windows src
    JOIN stores b
        ON src.store_id = b.id
),
with_dims_inventory AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.last_restock,
        a.region,
        a.city,
        s.opened_date AS store_opened_date,
        s.sqft,
        s.city AS store_city_2
    FROM with_dims a
    JOIN stores s
        ON a.store_id = s.id
)
SELECT
    s.id,
    s.store_id,
    s.last_restock,
    s.region,
    s.city,
    s.store_opened_date,
    s.sqft,
    s.store_city_2,
    (SELECT AVG(s2.revenue) FROM store_sales s2 WHERE s2.store_id = s.store_id) AS avg_store_sale_revenue
FROM with_dims_inventory s
ORDER BY last_restock DESC;
