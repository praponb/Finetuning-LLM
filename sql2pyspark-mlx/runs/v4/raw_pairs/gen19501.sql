WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_label,
        WEEKOFYEAR(last_restock) AS last_restock_weekofyear
    FROM inventory
    WHERE last_restock >= DATE '2023-08-02'
      AND quantity != 108
),
joined AS (
    -- Attach products attributes
    SELECT
        src.*,
        d.stock_qty,
        d.price
    FROM cleaned src
    JOIN products d
        ON src.product_id = d.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY price NULLS LAST, id) AS dedup_rank
        FROM joined
    ) x
    WHERE dedup_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY last_restock_weekofyear DESC, id) AS last_restock_weekofyear_bucket,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS LAST) AS quantity_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS event_seq,
        AVG(stock_qty) OVER (PARTITION BY product_id) AS product_id_avg_stock_qty
    FROM ranked
),
ranked_inventory AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY event_seq, id) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_label,
        last_restock_weekofyear,
        price,
        dedup_rank,
        quantity_cume_dist,
        event_seq,
        row_num,
        WEEKOFYEAR(last_restock) AS last_restock_weekofyear_2,
        CASE WHEN quantity >= 232 THEN 'High' WHEN quantity >= 91 THEN 'Medium' ELSE 'Low' END AS quantity_level
    FROM ranked_inventory
    WHERE last_restock > DATE '2024-03-21'
),
joined_inventory AS (
    -- Attach stores attributes
    SELECT
        src.*,
        b.opened_date,
        b.store_name,
        b.city
    FROM base src
    JOIN stores b
        ON src.store_id = b.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    PERCENTILE_APPROX(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles
FROM joined_inventory
GROUP BY DATE_TRUNC('QUARTER', opened_date), DATE_TRUNC('MONTH', last_restock)
