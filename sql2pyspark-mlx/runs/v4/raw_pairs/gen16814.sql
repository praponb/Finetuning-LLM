WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating NULLS LAST, id) AS rn
    FROM suppliers
    QUALIFY rn = 1
),
metrics AS (
    SELECT
        *,
        LAG(rating, 1, 0) OVER (PARTITION BY country, city ORDER BY rn, id) AS prev_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city, country), 4) AS rating_share
    FROM ranked
),
scored AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY country ORDER BY rating DESC, id) AS rating_bucket,
        COUNT(prev_rating) OVER w_ordered AS country_count_prev_rating,
        AVG(prev_rating) OVER (PARTITION BY country ORDER BY prev_rating, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS prev_rating_avg_6,
        CUME_DIST() OVER (PARTITION BY city ORDER BY rating_share NULLS LAST) AS rating_share_cume_dist,
        NTILE(5) OVER (PARTITION BY city ORDER BY rating_share, id) AS rating_share_bucket
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY country)
),
with_latest AS (
    SELECT s.*, top1.product_stock_qty
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
base AS (
    SELECT
        *,
        rating - product_stock_qty AS rating_minus_product_stock_qty,
        COALESCE(city, 'N/A') AS city_filled
    FROM with_latest
    WHERE country = 'US'
),
enriched AS (
    SELECT
        *,
        LEAD(prev_rating_avg_6, 2) OVER (PARTITION BY city ORDER BY rating, id) AS next_prev_rating_avg_6,
        FIRST_VALUE(rating_share_bucket) IGNORE NULLS OVER (PARTITION BY city ORDER BY rating_share_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating_share_bucket,
        AVG(country_count_prev_rating) OVER (PARTITION BY city ORDER BY rating_share, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS country_count_prev_rating_avg_3_prior,
        PERCENT_RANK() OVER (ORDER BY prev_rating NULLS LAST) AS prev_rating_percent_rank,
        SUM(country_count_prev_rating) OVER (ORDER BY prev_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_country_count_prev_rating
    FROM base
),
latest_match AS (
    SELECT
        s.*,
        top1.cost,
        top1.stock_qty
    FROM enriched s
    JOIN LATERAL (
            SELECT c.cost, c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    CASE WHEN cost >= 750 THEN 'HIGH' ELSE 'LOW' END AS cost_tier,
    country,
    CASE WHEN prev_rating_percent_rank >= 0.86 THEN 'LARGE' ELSE 'SMALL' END AS prev_rating_percent_rank_tier,
    MAX(country_count_prev_rating_avg_3_prior) FILTER (WHERE country = 'DE') AS de_max_country_count_prev_rating_avg_3_prior,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    SIZE(COLLECT_SET(country)) AS num_countries,
    GROUPING_ID() AS grouping_id
FROM latest_match
GROUP BY ROLLUP (CASE WHEN cost >= 750 THEN 'HIGH' ELSE 'LOW' END, country, CASE WHEN prev_rating_percent_rank >= 0.86 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY prev_rating_percent_rank_tier
