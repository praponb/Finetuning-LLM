WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(units, 0) AS units_rounded
    FROM store_sales
    WHERE revenue BETWEEN 1000 AND 2200
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(units) OVER (PARTITION BY product_id, store_id ORDER BY units ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_units,
        COUNT(units) OVER (PARTITION BY product_id, store_id ORDER BY units ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_units,
        MIN(units) OVER (PARTITION BY product_id, store_id ORDER BY units ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_units,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id, store_id), 4) AS revenue_share
    FROM valid
    WHERE units_rounded <= 74
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        b.product_name,
        b.launch_date,
        b.price
    FROM metrics a
    LEFT JOIN products b
        ON a.product_id = b.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.*,
        d.sqft,
        d.opened_date
    FROM with_dims e
    INNER JOIN stores d
        ON e.store_id = d.id
),
enriched_store_sales AS (
    -- Running and moving aggregates
    SELECT
        id,
        sale_date,
        units,
        units_rounded,
        running_sum_units,
        running_min_units,
        sqft,
        opened_date,
        AVG(units) OVER (PARTITION BY store_id, product_id ORDER BY launch_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS units_avg_2_prior,
        COUNT(units_rounded) OVER w_grp AS product_id_count_units_rounded,
        NTILE(10) OVER (ORDER BY running_min_units DESC, id ASC) AS running_min_units_bucket,
        SUM(price) OVER w_part AS store_id_sum_price,
        SUM(units) OVER (ORDER BY running_sum_units ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units
    FROM enriched
    WINDOW w_grp AS (PARTITION BY product_id), w_part AS (PARTITION BY store_id)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(running_sum_units AS INT) AS running_sum_units_int
    FROM enriched_store_sales
    WHERE opened_date >= '2023-10-01'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    CASE WHEN cumulative_units >= 207 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_units_tier,
    DATE_TRUNC('MONTH', sale_date) AS activity_month,
    PERCENTILE_APPROX(running_sum_units, ARRAY(0.25, 0.5, 0.75)) AS running_sum_units_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(units_rounded) AS min_units_rounded
FROM base
GROUP BY DATE_TRUNC('WEEK', opened_date), CASE WHEN cumulative_units >= 207 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', sale_date)
HAVING MIN(units_rounded) > 0
