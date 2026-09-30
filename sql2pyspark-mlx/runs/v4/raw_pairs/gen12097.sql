WITH enriched AS (
    -- Attach stores attributes
    SELECT
        src.*,
        s.sqft,
        s.store_name
    FROM store_sales src
    INNER JOIN stores s
        ON src.store_id = s.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        sqft,
        store_name,
        FIRST_VALUE(revenue, TRUE) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_revenue,
        ROW_NUMBER() OVER w_grp event_seq,
        AVG(revenue) OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) revenue_avg_6_prior,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) revenue_avg_5_prior
    FROM enriched
    WINDOW w_grp AS (ORDER BY units DESC, id)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(first_known_revenue / SUM(first_known_revenue) OVER (PARTITION BY product_id), 4) first_known_revenue_share,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY first_known_revenue DESC, id) row_seq,
        CUME_DIST() OVER (PARTITION BY product_id, store_id ORDER BY sqft) sqft_cume_dist
    FROM with_windows
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY event_seq NULLS LAST, id) AS dedup_rank
        FROM scored
    ) x
    WHERE dedup_rank = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(TRIM(store_name)) store_name_normalized
    FROM latest
    WHERE units > 16
),
joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        d.product_name,
        d.launch_date
    FROM cleaned s
    LEFT JOIN products d
        ON s.product_id = d.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT store_id, id
    FROM joined
    EXCEPT DISTINCT
    SELECT store_id, id
    FROM store_sales
    WHERE units IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN kept_keys k ON s.store_id = k.store_id AND s.id = k.id
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.sale_date,
    s.units,
    s.revenue,
    s.sqft,
    s.store_name,
    s.first_known_revenue,
    (SELECT AVG(t2.quantity) FROM inventory t2 WHERE t2.product_id = s.product_id) AS avg_inventory_quantity
FROM kept_rows s
ORDER BY id DESC, sale_date
