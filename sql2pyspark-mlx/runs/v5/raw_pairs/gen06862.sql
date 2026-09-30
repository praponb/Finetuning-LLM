WITH valid AS (
    SELECT
        id,
        product_name,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        UPPER(category) AS category_upper,
        DATE_TRUNC('month', launch_date) AS launch_date_period
    FROM products
    WHERE brand IS NOT NULL
      AND cost BETWEEN 260 AND 410
),
gaps AS (
    SELECT *, CASE WHEN LAG(launch_date_period) OVER (PARTITION BY supplier_id ORDER BY launch_date_period, id) IS NULL
                 OR launch_date_period > LAG(launch_date_period) OVER (PARTITION BY supplier_id ORDER BY launch_date_period, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY supplier_id ORDER BY launch_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
enriched AS (
    SELECT
        src.id,
        src.product_name,
        src.price,
        src.cost,
        src.launch_date,
        src.supplier_id,
        src.category_upper,
        src.launch_date_period,
        src.is_new_session,
        p.supplier_name,
        p.rating,
        p.country
    FROM sessionized src
    LEFT JOIN suppliers p
        ON src.supplier_id = p.id
),
enriched_products AS (
    SELECT
        *,
        AVG(rating) OVER (PARTITION BY supplier_id ORDER BY cost, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS rating_avg_5_prior,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC NULLS LAST) AS rating_dense_rank,
        RANK() OVER (ORDER BY is_new_session DESC NULLS FIRST) AS is_new_session_rank,
        LEAD(rating, 1) OVER (PARTITION BY supplier_id ORDER BY cost, id) AS next_rating,
        CORR(cost, price) OVER (PARTITION BY country) AS corr_cost_price
    FROM enriched
),
with_dims AS (
    SELECT
        a.*,
        b.city,
        b.rating AS supplier_rating
    FROM enriched_products a
    JOIN suppliers b
        ON a.supplier_id <=> b.id
    WHERE b.rating BETWEEN 1.8 AND 2.8
)
SELECT
    country,
    DATE_TRUNC('YEAR', launch_date_period) AS year_start,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(next_rating), 2) AS avg_next_rating,
    COUNT(*) FILTER (WHERE city = 'New York') AS new_york_rows
FROM with_dims
GROUP BY country, DATE_TRUNC('YEAR', launch_date_period)
ORDER BY avg_next_rating DESC
