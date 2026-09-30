WITH with_latest AS (
    SELECT
        s.*,
        top1.product_launch_date,
        top1.product_cost
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date, c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY product_launch_date DESC, id) AS event_seq
    FROM with_latest
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER w_part AS rating_cume_dist,
        FIRST_VALUE(rating, TRUE) OVER (PARTITION BY country ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM metrics
    WINDOW w_part AS (ORDER BY rating NULLS LAST), w_grp AS (PARTITION BY country ORDER BY product_launch_date DESC, id)
)
SELECT
    DATE_TRUNC('WEEK', product_launch_date) AS week_start,
    city,
    SIZE(COLLECT_SET(country)) AS num_countries,
    MIN(rating) AS min_rating,
    COUNT_IF(country = 'US') AS us_events,
    MAX(rating_share) FILTER (WHERE country = 'JP') AS jp_max_rating_share,
    APPROX_PERCENTILE(product_cost, ARRAY(0.25, 0.5, 0.75)) AS product_cost_quartiles
FROM with_windows
GROUP BY DATE_TRUNC('WEEK', product_launch_date), city;
