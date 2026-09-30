WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        city,
        rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        SUM(rating) OVER (PARTITION BY city) AS city_sum_rating
    FROM suppliers
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        ROW_NUMBER() OVER w_part AS event_seq,
        PERCENT_RANK() OVER (PARTITION BY country, city ORDER BY rating DESC NULLS FIRST) AS rating_percent_rank,
        ROW_NUMBER() OVER (ORDER BY city_sum_rating DESC, id) AS row_seq
    FROM scored
    WINDOW w_part AS (PARTITION BY city, country ORDER BY rating_share DESC, id)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.launch_date BETWEEN '2024-06-01' AND '2025-03-25'
        )
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.event_seq,
        s.rating_percent_rank,
        latest.product_category,
        latest.price
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.category AS product_category, c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored_suppliers AS (
    -- Running and moving aggregates
    SELECT
        id,
        price,
        SUM(rating_percent_rank) OVER (PARTITION BY product_category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_rating_percent_rank,
        COUNT(rating_percent_rank) OVER (PARTITION BY product_category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_rating_percent_rank,
        MIN(rating_percent_rank) OVER (PARTITION BY product_category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_rating_percent_rank,
        AVG(rating_percent_rank) OVER (PARTITION BY product_category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_rating_percent_rank,
        STDDEV_SAMP(event_seq) OVER (PARTITION BY product_category) AS product_category_stddev_samp_event_seq
    FROM latest_match
),
scored_suppliers_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(running_sum_rating_percent_rank) OVER w_grp AS last3_sum_running_sum_rating_percent_rank,
        MIN(running_sum_rating_percent_rank) OVER w_grp AS last3_min_running_sum_rating_percent_rank,
        MAX(running_sum_rating_percent_rank) OVER w_grp AS last3_max_running_sum_rating_percent_rank
    FROM scored_suppliers
    WINDOW w_grp AS (ORDER BY running_avg_rating_percent_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    price,
    running_sum_rating_percent_rank,
    running_count_rating_percent_rank,
    running_min_rating_percent_rank,
    last3_sum_running_sum_rating_percent_rank,
    last3_min_running_sum_rating_percent_rank,
    last3_max_running_sum_rating_percent_rank
FROM scored_suppliers_2
ORDER BY id DESC;
