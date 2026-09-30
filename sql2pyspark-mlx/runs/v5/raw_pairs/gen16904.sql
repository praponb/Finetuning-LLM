WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(country, 1, 4) AS country_prefix,
        COALESCE(rating, 0) AS rating_filled
    FROM suppliers
    WHERE city IS NOT NULL
      AND city = 'New York'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND LENGTH(c.product_name) > 5
        )
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        supplier_name,
        city,
        rating_filled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating_filled DESC NULLS FIRST, id) AS latest_rank
        FROM eligible
    ) t
    WHERE latest_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(rating_filled) OVER (PARTITION BY city ORDER BY rating_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating_filled,
        CUME_DIST() OVER (PARTITION BY city ORDER BY rating_filled DESC NULLS FIRST) AS rating_filled_cume_dist,
        AVG(rating_filled) OVER w_ordered AS rating_filled_avg_2_prior
    FROM deduped
    WINDOW w_ordered AS (PARTITION BY city ORDER BY rating_filled, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        city,
        rating_filled_cume_dist,
        rating_filled
    FROM with_windows
) src
UNPIVOT (
    metric_value FOR metric_name IN (rating_filled_cume_dist AS RATING_FILLED_CUME_DIST, rating_filled)
);
