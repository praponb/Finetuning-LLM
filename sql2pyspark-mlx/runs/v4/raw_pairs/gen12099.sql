WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        LEAST(price, stock_qty) AS least_price_stock_qty,
        price * stock_qty AS price_times_stock_qty,
        COALESCE(brand, 'OTHER') AS brand_filled
    FROM products
    WHERE launch_date >= TO_DATE('2023-05-25')
      AND brand IS NOT NULL
      AND ARRAY_CONTAINS(tags, 'clearance')
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price_times_stock_qty DESC NULLS FIRST, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date DESC, id) AS event_seq,
        PERCENT_RANK() OVER w_ordered AS price_times_stock_qty_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY price_times_stock_qty, id) AS row_seq,
        ROW_NUMBER() OVER w_recent AS event_seq_2
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY supplier_id ORDER BY price_times_stock_qty DESC), w_recent AS (PARTITION BY category ORDER BY stock_qty DESC, id)
),
totals AS (
    -- Multi-level aggregation
    SELECT
        supplier_id,
        CASE WHEN price_times_stock_qty >= 71 THEN 'HIGH' ELSE 'LOW' END AS price_times_stock_qty_tier,
        DATE_TRUNC('WEEK', launch_date) AS week_start,
        COUNT(CASE WHEN category = 'Fashion' THEN 1 END) AS fashion_count,
        ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN category = 'Grocery' THEN 1 END), 0), 2) AS event_seq_per_grocery,
        SIZE(COLLECT_SET(category)) AS num_categories
    FROM enriched
    GROUP BY supplier_id, price_times_stock_qty_tier, week_start
    HAVING SIZE(COLLECT_SET(category)) > 1
)
SELECT
    supplier_id,
    price_times_stock_qty_tier,
    week_start,
    fashion_count,
    event_seq_per_grocery,
    num_categories
FROM totals
ORDER BY supplier_id, week_start;
