WITH base AS (
    SELECT
        id,
        supplier_name,
        city,
        rating,
        CONCAT(country, ', ', supplier_name) country_supplier_name,
        COALESCE(city, 'NONE') resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
),
scored AS (
    SELECT
        id,
        supplier_name,
        city,
        rating,
        country_supplier_name,
        AVG(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) rating_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) seq_no,
        FIRST_VALUE(city) IGNORE NULLS OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_city
    FROM base
    WHERE city IN ('Berlin', 'London', 'New York')
),
not_flagged AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date >= '2023-05-01'
        )
),
scored_suppliers AS (
    SELECT
        *,
        LAG(rating, 2) OVER (PARTITION BY city ORDER BY seq_no, id) prev_rating,
        NTILE(10) OVER (PARTITION BY city ORDER BY seq_no, id) seq_no_bucket,
        AVG(rating_avg_6_prior) OVER (PARTITION BY city ORDER BY seq_no, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) rating_avg_6_prior_avg_2,
        NTILE(3) OVER (PARTITION BY city ORDER BY rating DESC, id) rating_bucket,
        SUM(rating_avg_6_prior) OVER (PARTITION BY city ORDER BY rating_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_rating_avg_6_prior
    FROM not_flagged
),
enriched AS (
    SELECT
        id,
        supplier_name,
        rating,
        country_supplier_name,
        seq_no_bucket,
        rating_avg_6_prior_avg_2,
        rating_bucket,
        running_rating_avg_6_prior,
        SUM(rating_avg_6_prior_avg_2) OVER w_grp cumulative_rating_avg_6_prior_avg_2,
        ROW_NUMBER() OVER (ORDER BY seq_no, id) event_seq
    FROM scored_suppliers
    WINDOW w_grp AS (PARTITION BY city ORDER BY rating_avg_6_prior_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    supplier_name,
    country_supplier_name,
    rating_avg_6_prior_avg_2,
    event_seq
FROM enriched
ORDER BY event_seq DESC;
