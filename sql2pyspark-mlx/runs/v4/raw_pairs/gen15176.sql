WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units DESC, id) AS latest_rank
        FROM store_sales
    ) t
    WHERE latest_rank = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY units DESC NULLS LAST, id) AS rn
    FROM latest
    QUALIFY rn = 1
),
latest_store_sales AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY units NULLS LAST) AS dedup_rank
        FROM deduped
    ) ranked
    WHERE dedup_rank = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.brand,
        p.launch_date
    FROM latest_store_sales src
    LEFT JOIN products p
        ON src.product_id = p.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(revenue, 1) OVER (PARTITION BY product_id ORDER BY latest_rank, id) AS next_revenue,
        RANK() OVER w_running AS revenue_rank
    FROM with_dims
    WINDOW w_running AS (PARTITION BY store_id ORDER BY revenue DESC)
)
SELECT
    id,
    store_id,
    sale_date,
    units,
    brand,
    launch_date,
    revenue_rank,
    CASE WHEN brand RLIKE '^(Umbrella|Acme)$' THEN 'CORE' WHEN brand RLIKE '^(Globex|Initech)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS brand_zone
FROM scored
ORDER BY launch_date DESC NULLS LAST
