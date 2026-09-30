WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(rating) AS rating_whole,
        SUBSTRING(country, 1, 4) AS country_prefix
    FROM suppliers
    WHERE city IS NOT NULL
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.rating_whole,
        s.country_prefix,
        top1.product_stock_qty
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        rating_whole,
        AVG(product_stock_qty) OVER (ORDER BY rating_whole, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS product_stock_qty_avg_2,
        PERCENT_RANK() OVER (ORDER BY rating_whole DESC NULLS LAST) AS rating_whole_percent_rank,
        AVG(product_stock_qty) OVER (ORDER BY rating_whole, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS product_stock_qty_avg_2_prior
    FROM with_last
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        rating_whole,
        product_stock_qty_avg_2,
        rating_whole_percent_rank,
        product_stock_qty_avg_2_prior,
        MIN(rating_whole) OVER (ORDER BY rating_whole, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_rating_whole,
        SUM(rating_whole) OVER (ORDER BY rating_whole, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_rating_whole,
        COUNT(rating_whole) OVER (ORDER BY rating_whole, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_rating_whole,
        CUME_DIST() OVER (ORDER BY rating_whole_percent_rank NULLS FIRST) AS rating_whole_percent_rank_cume_dist
    FROM with_windows
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN rating_whole >= 73 THEN 'High' WHEN rating_whole >= 25 THEN 'Medium' ELSE 'Low' END AS rating_whole_band,
        CASE WHEN last3_count_rating_whole >= 18 THEN 'High' WHEN last3_count_rating_whole >= 8 THEN 'Medium' ELSE 'Low' END AS last3_count_rating_whole_tier
    FROM scored
    WHERE last3_sum_rating_whole < 160
)
-- Ranking and gap features
SELECT
    id,
    product_stock_qty_avg_2,
    rating_whole_percent_rank,
    last3_sum_rating_whole,
    last3_count_rating_whole,
    last3_count_rating_whole_tier,
    ROW_NUMBER() OVER (ORDER BY product_stock_qty_avg_2, id) AS event_seq,
    LAG(rating_whole_percent_rank_cume_dist, 1, 0) OVER (ORDER BY rating_whole_percent_rank_cume_dist, id) AS prev_rating_whole_percent_rank_cume_dist,
    RANK() OVER (ORDER BY last3_min_rating_whole DESC NULLS LAST) AS last3_min_rating_whole_rank,
    FIRST_VALUE(rating_whole_percent_rank, TRUE) OVER (ORDER BY last3_min_rating_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating_whole_percent_rank
FROM valid;
