WITH valid AS (
    SELECT *, CEIL(rating) AS rating_whole
    FROM suppliers
    WHERE LENGTH(supplier_name) >= 7
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating ASC, id ASC) AS rn
    FROM valid
    QUALIFY rn = 1
),
eligible AS (
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND ARRAY_CONTAINS(x.tags, 'new')
        )
),
base AS (
    SELECT
        id,
        rating,
        rn,
        ROUND(rating * 0.85, 2) AS adjusted_rating
    FROM eligible
    WHERE city IS NOT NULL
),
eligible_suppliers AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category IN ('Electronics', 'Home', 'Grocery')
        )
)
SELECT
    *,
    SUM(adjusted_rating) OVER (ORDER BY rn ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_adjusted_rating,
    SUM(adjusted_rating) OVER (ORDER BY adjusted_rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_adjusted_rating
FROM eligible_suppliers
WHERE rating >= 2.1
