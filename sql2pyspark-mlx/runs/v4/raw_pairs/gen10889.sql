WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATE_ADD(sale_date, 78) AS sale_date_plus_78d
    FROM store_sales
    WHERE revenue >= 3900
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) = 1
),
with_dims AS (
    -- Attach stores attributes
    SELECT src.*, p.region
    FROM latest src
    INNER JOIN stores p
        ON src.store_id = p.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN units >= 39 THEN 'LARGE' ELSE 'SMALL' END AS units_tier,
    MAX(revenue) FILTER (WHERE region = 'North') AS north_max_revenue,
    CORR(revenue, units) AS corr_revenue_units,
    COUNT(CASE WHEN region = 'South' THEN 1 END) AS south_count,
    BIT_OR(units) AS units_bit_or,
    COUNT(*) AS cnt,
    COUNT_IF(region = 'West') AS west_events
FROM with_dims
GROUP BY CASE WHEN units >= 39 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY cnt DESC NULLS FIRST;
