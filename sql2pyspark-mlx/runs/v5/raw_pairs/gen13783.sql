WITH filtered AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CASE WHEN rating >= 3.6 THEN 'High' WHEN rating >= 2.1 THEN 'Medium' ELSE 'Low' END AS rating_band
    FROM suppliers
    WHERE city IS DISTINCT FROM 'London'
      AND city IS NOT NULL
),
cleaned AS (
    SELECT
        id,
        supplier_name,
        rating,
        rating_band,
        COALESCE(rating, 0) AS rating_filled
    FROM filtered
    WHERE supplier_name IS NOT NULL
),
scored AS (
    SELECT
        *,
        RANK() OVER w_grp AS rating_rank,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM cleaned
    WINDOW w_grp AS (ORDER BY rating DESC NULLS FIRST), w_recent AS (ORDER BY rating_filled DESC, id)
),
enriched AS (
    SELECT
        id,
        rating,
        rating_band,
        rating_rank,
        AVG(row_seq) OVER (ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS row_seq_avg_6_prior,
        SUM(rating_filled) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating_filled
    FROM scored
),
scored_suppliers AS (
    SELECT
        id,
        rating_rank,
        FIRST_VALUE(cumulative_rating_filled) IGNORE NULLS OVER (ORDER BY row_seq_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_rating_filled,
        DENSE_RANK() OVER (ORDER BY row_seq_avg_6_prior) AS row_seq_avg_6_prior_dense_rank,
        DENSE_RANK() OVER (ORDER BY row_seq_avg_6_prior DESC NULLS LAST) AS row_seq_avg_6_prior_dense_rank_2
    FROM enriched
    WHERE cumulative_rating_filled BETWEEN 113 AND 197
),
with_windows AS (
    SELECT
        id,
        first_known_cumulative_rating_filled,
        LAST_VALUE(rating_rank, TRUE) OVER (ORDER BY first_known_cumulative_rating_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating_rank,
        LAST_VALUE(first_known_cumulative_rating_filled, TRUE) OVER (ORDER BY row_seq_avg_6_prior_dense_rank_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_known_cumulative_rating_filled,
        RANK() OVER (ORDER BY rating_rank DESC NULLS FIRST) AS rating_rank_rank,
        LEAD(row_seq_avg_6_prior_dense_rank_2, 1) OVER (ORDER BY first_known_cumulative_rating_filled, id) AS next_row_seq_avg_6_prior_dense_rank_2,
        LAST_VALUE(rating_rank) IGNORE NULLS OVER (ORDER BY row_seq_avg_6_prior_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating_rank_2
    FROM scored_suppliers
),
eligible AS (
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.product_name NOT LIKE 'O%'
        )
)
SELECT
    CASE WHEN last_known_rating_rank_2 >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_rating_rank_2_tier,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(first_known_cumulative_rating_filled, 0.9) AS first_known_cumulative_rating_filled_p90,
    ROUND(AVG(last_known_rating_rank_2), 2) AS avg_last_known_rating_rank_2,
    ROUND(AVG(next_row_seq_avg_6_prior_dense_rank_2), 2) AS avg_next_row_seq_avg_6_prior_dense_rank_2
FROM eligible
GROUP BY 1
ORDER BY num_rows
