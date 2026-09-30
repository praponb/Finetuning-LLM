WITH base AS (
    SELECT
        *,
        LOWER(country) AS country_lower,
        COALESCE(city, 'NONE') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
      AND rating > 3.8
),
latest_match AS (
    SELECT s.*, top1.product_launch_date
    FROM base s
    JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_running AS seq_no,
        SUM(rating) OVER (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        AVG(rating) OVER w_part AS rating_avg_3
    FROM latest_match
    WINDOW w_running AS (PARTITION BY country ORDER BY rating DESC, id), w_part AS (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
not_flagged AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date >= s.product_launch_date
        )
),
eligible AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date < s.product_launch_date
        )
),
metrics_suppliers AS (
    SELECT
        *,
        STDDEV_SAMP(seq_no) OVER (PARTITION BY country) AS country_stddev_samp_seq_no,
        LEAD(seq_no, 1, 0) OVER (ORDER BY product_launch_date, id) AS next_seq_no
    FROM eligible
),
grouped AS (
    SELECT
        CASE WHEN next_seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END AS next_seq_no_tier,
        country,
        DATE_TRUNC('WEEK', product_launch_date) AS week_start,
        SIZE(COLLECT_SET(city)) AS num_cities,
        ROUND(AVG(running_rating), 2) AS avg_running_rating,
        SIZE(COLLECT_SET(city)) AS num_cities_2,
        SUM(next_seq_no) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_total_next_seq_no,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        ROUND(AVG(rating_avg_3), 2) AS avg_rating_avg_3,
        GROUPING_ID() AS gid
    FROM metrics_suppliers
    GROUP BY CUBE (CASE WHEN next_seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END, country, DATE_TRUNC('WEEK', product_launch_date))
    HAVING num_cities_2 >= 1
)
SELECT
    next_seq_no_tier,
    country,
    week_start,
    avg_running_rating,
    num_cities_2,
    city_list,
    gid
FROM grouped
