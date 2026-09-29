WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS quantity_bucket,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY order_id), 4) AS quantity_share,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 4) AS quantity_share_2,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 4) AS unit_price_share
    FROM order_items
),
joined AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.category,
        p.price
    FROM with_windows src
    LEFT JOIN products p
        ON src.product_id = p.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM joined
    EXCEPT
    SELECT id
    FROM order_items
    WHERE product_id = 12
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.id = k.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        quantity_bucket,
        category,
        price,
        COUNT(first_known_unit_price) OVER w_grp AS category_count_first_known_unit_price,
        SUM(unit_price_share) OVER w_grp AS category_sum_unit_price_share
    FROM verified
    WINDOW w_grp AS (PARTITION BY category)
)
-- Ranking and gap features
SELECT
    category,
    price,
    category_count_first_known_unit_price,
    MIN(category_sum_unit_price_share) OVER (PARTITION BY category) AS category_min_category_sum_unit_price_share,
    ROUND(quantity_bucket / SUM(quantity_bucket) OVER (PARTITION BY category), 4) AS quantity_bucket_share,
    RANK() OVER (PARTITION BY category ORDER BY quantity_bucket DESC NULLS LAST) AS quantity_bucket_rank,
    DENSE_RANK() OVER w_grp AS category_sum_unit_price_share_dense_rank
FROM scored
WHERE category != 'Home'
WINDOW w_grp AS (PARTITION BY category ORDER BY category_sum_unit_price_share DESC NULLS LAST)
