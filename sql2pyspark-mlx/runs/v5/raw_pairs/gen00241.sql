WITH joined AS (
    -- Attach stores attributes
    SELECT
        a.*,
        p.sqft,
        p.region,
        p.store_name
    FROM store_sales a
    INNER JOIN stores p
        ON a.store_id = p.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM joined
    INTERSECT ALL
    SELECT id
    FROM store_sales
    WHERE revenue BETWEEN 2900 AND 3900
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.id = k.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        CORR(units, revenue) OVER (PARTITION BY store_id) AS corr_units_revenue,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id), 4) AS revenue_share
    FROM reconciled
    WHERE revenue < 2200
),
scored AS (
    -- Running and moving aggregates
    SELECT
        store_id,
        sqft,
        store_name,
        revenue_share,
        PERCENT_RANK() OVER (ORDER BY revenue) AS revenue_percent_rank
    FROM metrics
)
-- Join a dimension and aggregate
SELECT
    d.region,
    COUNT(*) AS rows_total,
    ROUND(AVG(a.sqft), 2) AS avg_sqft,
    ROUND(AVG(a.revenue_percent_rank), 2) AS avg_revenue_percent_rank
FROM scored a
INNER JOIN stores d ON a.store_id = d.id
GROUP BY ROLLUP (d.region)
ORDER BY rows_total NULLS LAST
