WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(rating) OVER w_ordered AS cumulative_rating,
        NTILE(3) OVER (PARTITION BY city ORDER BY rating ASC, id ASC) AS rating_bucket
    FROM suppliers
    WHERE rating BETWEEN 2.6 AND 2.7
    WINDOW w_ordered AS (PARTITION BY country, city ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        rating,
        cumulative_rating,
        rating + rating_bucket AS rating_plus_rating_bucket,
        REPLACE(supplier_name, ' ', '') AS supplier_name_slug
    FROM enriched
    WHERE city IS NOT NULL
      AND LOWER(TRIM(supplier_name)) = 'tokyo traders'
),
enriched_suppliers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(cumulative_rating) OVER w_part AS prev_cumulative_rating,
        LEAD(cumulative_rating) OVER w_part AS next_cumulative_rating,
        LEAD(rating) OVER w_part AS next_rating,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM cleaned
    WINDOW w_part AS (ORDER BY rating_plus_rating_bucket ASC, id ASC)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(rating, TRUE) OVER (ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating,
        CUME_DIST() OVER (ORDER BY prev_cumulative_rating ASC NULLS FIRST) AS prev_cumulative_rating_cume_dist,
        FIRST_VALUE(next_rating) IGNORE NULLS OVER (ORDER BY next_rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_rating,
        LAG(next_rating, 1) OVER (ORDER BY seq_no ASC, id ASC) AS prev_next_rating
    FROM enriched_suppliers
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        rating,
        rating_plus_rating_bucket,
        supplier_name_slug,
        prev_cumulative_rating,
        next_rating,
        seq_no,
        last_known_rating,
        prev_cumulative_rating_cume_dist,
        first_known_next_rating,
        prev_next_rating,
        SUM(next_cumulative_rating) OVER (ORDER BY last_known_rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_cumulative_rating,
        LAST_VALUE(cumulative_rating) IGNORE NULLS OVER w_ordered AS last_known_cumulative_rating
    FROM with_windows
    WINDOW w_ordered AS (ORDER BY prev_cumulative_rating_cume_dist ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    rating,
    rating_plus_rating_bucket,
    prev_cumulative_rating,
    next_rating,
    last_known_rating,
    prev_cumulative_rating_cume_dist,
    first_known_next_rating
FROM metrics
ORDER BY rating ASC
