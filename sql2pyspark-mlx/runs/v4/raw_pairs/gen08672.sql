WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(units) OVER (PARTITION BY store_id) AS store_id_avg_units,
        AVG(units) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS units_avg_5_prior
    FROM store_sales
),
with_dims AS (
    -- Attach stores attributes
    SELECT s.*, d.city
    FROM metrics s
    LEFT JOIN stores d
        ON s.store_id = d.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p2.store_name,
        p2.sqft,
        p2.opened_date
    FROM with_dims s
    INNER JOIN stores p2
        ON s.store_id = p2.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY store_id_avg_units NULLS FIRST, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    store_id,
    product_id,
    units,
    revenue,
    units_avg_5_prior,
    store_name,
    sqft,
    dedup_rank
FROM deduped
ORDER BY store_id DESC NULLS LAST
