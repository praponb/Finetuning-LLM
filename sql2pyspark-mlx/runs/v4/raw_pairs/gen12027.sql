WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(revenue, 0) revenue_filled
    FROM store_sales
    WHERE sale_date > DATE '2024-04-11'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue_filled,
        ROUND(revenue_filled / units, 2) revenue_filled_per_units,
        GREATEST(revenue, units) greatest_revenue_units
    FROM filtered
    WHERE sale_date > DATE '2023-05-04'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue_filled,
        revenue_filled_per_units,
        greatest_revenue_units,
        AVG(units) OVER w_part store_id_avg_units,
        RANK() OVER (PARTITION BY store_id ORDER BY revenue_filled_per_units DESC NULLS FIRST) revenue_filled_per_units_rank
    FROM valid
    WINDOW w_part AS (PARTITION BY store_id)
)
SELECT id, units
FROM scored
