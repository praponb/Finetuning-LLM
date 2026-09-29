WITH with_dims AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.units,
        s.store_name,
        s.sqft,
        s.opened_date
    FROM store_sales a
    LEFT JOIN stores s
        ON a.store_id = s.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC NULLS FIRST, id) AS rank_in_group
        FROM with_dims
    ) x
    WHERE rank_in_group = 1
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.sqft,
        e.opened_date,
        d.brand,
        d.category,
        d.cost
    FROM ranked e
    JOIN products d
        ON e.product_id = d.id
),
ranked_store_sales AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY cost, id) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
)
SELECT
    id,
    product_id,
    sqft,
    opened_date,
    brand,
    cost,
    latest_rank
FROM ranked_store_sales
ORDER BY product_id DESC
