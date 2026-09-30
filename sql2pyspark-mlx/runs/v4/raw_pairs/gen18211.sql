WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        DATE_SUB(launch_date, 36) AS launch_date_minus_36d,
        REGEXP_REPLACE(product_name, ' ', '_') AS product_name_slug
    FROM products
    WHERE stock_qty BETWEEN 101 AND 275
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date_minus_36d DESC, id) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date_minus_36d DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date_minus_36d DESC, id) AS seq_no,
        FIRST_VALUE(brand) IGNORE NULLS OVER (PARTITION BY category ORDER BY launch_date_minus_36d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_brand,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date_minus_36d, id) AS event_seq_2,
        FIRST_VALUE(brand) IGNORE NULLS OVER (PARTITION BY brand ORDER BY launch_date_minus_36d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_brand_2
    FROM latest
),
scored_products AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY seq_no DESC, id) AS seq_no_2,
        SUM(seq_no) OVER (PARTITION BY category ORDER BY launch_date_minus_36d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
        LAG(event_seq_2, 1, 0) OVER (ORDER BY event_seq_2, id) AS prev_event_seq_2
    FROM scored
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored_products s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.last_restock >= s.launch_date_minus_36d
        )
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.store_sale_revenue
    FROM eligible s
    JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date <= s.launch_date_minus_36d
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match_products AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.last_restock
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date_minus_36d
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    category,
    brand,
    product_name_slug,
    seq_no,
    first_known_brand,
    first_known_brand_2,
    running_seq_no,
    prev_event_seq_2
FROM latest_match_products
