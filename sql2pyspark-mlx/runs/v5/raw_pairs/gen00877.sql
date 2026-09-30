WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        ROUND(units / revenue, 2) AS revenue_per_unit,
        ROUND(units / revenue, 2) AS revenue_per_unit_2
    FROM store_sales
    WHERE store_id = 3
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.*,
        s.city,
        s.sqft,
        s.region
    FROM filtered e
    INNER JOIN stores s
        ON e.store_id = s.id
    WHERE s.sqft > 14647
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue_per_unit, id) AS row_num
    FROM joined
    QUALIFY row_num = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue_per_unit,
        revenue_per_unit_2,
        sqft,
        region,
        LAG(revenue_per_unit_2) OVER w_recent AS prev_revenue_per_unit_2,
        LEAD(revenue_per_unit_2) OVER w_recent AS next_revenue_per_unit_2,
        LEAD(units) OVER w_recent AS next_units,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM ranked
    WINDOW w_recent AS (PARTITION BY city, region ORDER BY sqft, id)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region, store_id ORDER BY units DESC, id) AS rn
    FROM scored
    QUALIFY rn = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    BIT_AND(units) AS units_bit_and,
    SUM(revenue_per_unit_2) FILTER (WHERE region = 'East') AS east_total_revenue_per_unit_2,
    SIZE(COLLECT_SET(region)) AS num_regions,
    COUNT_IF(region = 'North') AS north_events,
    COUNT(DISTINCT id) AS distinct_ids
FROM latest
GROUP BY CUBE (DATE_TRUNC('QUARTER', sale_date));
