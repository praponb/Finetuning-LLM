WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE revenue >= 2600
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        sale_date,
        units,
        sale_date_month_end,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY revenue ASC, id ASC) AS revenue_bucket,
        CORR(revenue, units) OVER (PARTITION BY store_id) AS corr_revenue_units,
        LAG(revenue, 1) OVER (PARTITION BY store_id, product_id ORDER BY units ASC, id ASC) AS prev_revenue,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id, store_id), 4) AS revenue_share,
        NTILE(5) OVER (ORDER BY revenue ASC, id ASC) AS revenue_bucket_2
    FROM filtered
    WHERE revenue < 1100
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id ASC) AS rn
    FROM enriched
    QUALIFY rn = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date ASC NULLS LAST, id ASC) = 1
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.*,
        s.sqft,
        s.region,
        s.opened_date
    FROM deduped a
    LEFT JOIN stores s
        ON a.store_id = s.id
)
SELECT
    id,
    store_id,
    sale_date,
    units,
    sale_date_month_end,
    corr_revenue_units,
    revenue_share,
    revenue_bucket_2,
    region,
    CASE WHEN region RLIKE '^(West|Central)$' THEN 'OTHER_ZONE' WHEN region RLIKE '^(North|South)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS region_zone
FROM with_dims
ORDER BY sale_date_month_end DESC
