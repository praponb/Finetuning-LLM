WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, quantity * unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND product_id = 10
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        unit_price,
        CEIL(unit_price) AS unit_price_whole
    FROM ranked
    WHERE product_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        unit_price,
        unit_price_whole,
        AVG(unit_price_whole) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS unit_price_whole_avg_3,
        FIRST_VALUE(unit_price_whole, TRUE) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_whole
    FROM base
),
valid_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        unit_price,
        unit_price_whole,
        first_known_unit_price_whole,
        CASE WHEN first_known_unit_price_whole >= 83 THEN 'High' WHEN first_known_unit_price_whole >= 15 THEN 'Medium' ELSE 'Low' END AS first_known_unit_price_whole_level
    FROM metrics
    WHERE unit_price_whole_avg_3 >= 56
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.amount,
        o.discount,
        o.shipping_fee
    FROM valid_order_items src
    JOIN orders o
        ON src.order_id = o.id
),
enriched_order_items AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        unit_price,
        unit_price_whole,
        first_known_unit_price_whole,
        first_known_unit_price_whole_level,
        amount,
        discount,
        shipping_fee,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY unit_price_whole DESC, id) AS unit_price_whole_bucket,
        SUM(first_known_unit_price_whole) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_first_known_unit_price_whole,
        ROW_NUMBER() OVER (ORDER BY unit_price, id) AS seq_no,
        LAG(first_known_unit_price_whole, 1, 0) OVER (PARTITION BY order_id ORDER BY first_known_unit_price_whole, id) AS prev_first_known_unit_price_whole,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY unit_price_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price
    FROM enriched
)
-- Running and moving aggregates
SELECT
    *,
    NTILE(4) OVER (ORDER BY unit_price_whole, id) AS unit_price_whole_bucket_2,
    AVG(discount) OVER (PARTITION BY order_id ORDER BY discount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS discount_avg_3_prior,
    AVG(unit_price) OVER (PARTITION BY order_id) AS order_id_avg_unit_price
FROM enriched_order_items;
