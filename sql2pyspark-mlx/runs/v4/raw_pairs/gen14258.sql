WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        CASE WHEN revenue >= 3900 THEN 'High' WHEN revenue >= 1900 THEN 'Medium' ELSE 'Low' END AS revenue_band,
        DATE_SUB(sale_date, 4) AS sale_date_minus_4d
    FROM store_sales
    WHERE sale_date > DATE '2023-10-27'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY product_id ORDER BY revenue NULLS FIRST) AS revenue_rank,
        CORR(units, revenue) OVER (PARTITION BY product_id) AS corr_units_revenue,
        LAST_VALUE(units, TRUE) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units,
        AVG(units) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS units_avg_5
    FROM cleaned
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(units_avg_5) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_units_avg_5,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY corr_units_revenue, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY last_known_units, id) AS event_seq_2,
        AVG(corr_units_revenue) OVER (PARTITION BY product_id) AS product_id_avg_corr_units_revenue,
        CORR(units, revenue) OVER (PARTITION BY product_id) AS corr_units_revenue_2
    FROM enriched
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.stock_qty,
        p.cost,
        p.category
    FROM metrics e
    LEFT JOIN products p
        ON e.product_id = p.id
)
-- Running and moving aggregates
SELECT
    *,
    AVG(corr_units_revenue_2) OVER (PARTITION BY product_id ORDER BY first_known_units_avg_5, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS corr_units_revenue_2_avg_5,
    LAST_VALUE(event_seq) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY revenue_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_seq
FROM with_dims;
