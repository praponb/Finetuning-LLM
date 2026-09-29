WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        DATE_TRUNC('WEEK', sale_date) sale_date_period,
        ROUND(units / revenue, 2) revenue_per_unit
    FROM store_sales
    WHERE units IS NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(sale_date_period) OVER (PARTITION BY store_id ORDER BY sale_date_period ASC, id ASC) IS NULL
                 OR sale_date_period > LAG(sale_date_period) OVER (PARTITION BY store_id ORDER BY sale_date_period ASC, id ASC) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY store_id ORDER BY sale_date_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        sale_date,
        units,
        revenue,
        revenue_per_unit,
        is_new_burst
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY sale_date_period DESC NULLS FIRST, id ASC) AS dedup_rank
        FROM islands
    ) x
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(is_new_burst) OVER (ORDER BY units ASC, id ASC) prev_is_new_burst,
        LEAD(is_new_burst) OVER (ORDER BY units ASC, id ASC) next_is_new_burst,
        LAG(units) OVER (ORDER BY units ASC, id ASC) prev_units,
        LEAD(units) OVER (ORDER BY units ASC, id ASC) next_units,
        ROW_NUMBER() OVER (ORDER BY units ASC, id ASC) event_seq,
        DENSE_RANK() OVER (ORDER BY units DESC) units_dense_rank
    FROM ranked
    WHERE revenue <= 2400
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        revenue,
        revenue_per_unit
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR kpi IN (revenue, revenue_per_unit)
);
