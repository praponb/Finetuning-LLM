WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        ADD_MONTHS(sale_date, 3) AS sale_date_plus_months,
        COALESCE(units, 0) AS units_filled
    FROM store_sales
    WHERE store_id = 4
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        sale_date,
        units,
        LEAD(units, 1) OVER w_grp AS next_units,
        RANK() OVER w_running AS units_rank,
        AVG(units) OVER w_ordered AS units_avg_2
    FROM filtered
    WHERE store_id IN (1, 2, 5, 6)
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY sale_date_plus_months, id), w_running AS (ORDER BY units DESC NULLS FIRST), w_ordered AS (ORDER BY sale_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(units_avg_2 * 1.07, 2) AS units_avg_2_adjusted,
        QUARTER(sale_date) AS sale_date_quarter
    FROM with_windows
    WHERE units_avg_2 BETWEEN 16 AND 20
),
with_windows_store_sales AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        units,
        units_rank,
        units_avg_2,
        units_avg_2_adjusted,
        sale_date_quarter,
        AVG(units_rank) OVER (PARTITION BY store_id ORDER BY units_avg_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS units_rank_avg_3,
        FIRST_VALUE(units_avg_2) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY sale_date_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_units_avg_2,
        SUM(sale_date_quarter) OVER (PARTITION BY store_id ORDER BY units_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sale_date_quarter
    FROM valid
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_id,
        units_rank,
        units_rank_avg_3
    FROM with_windows_store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY first_known_units_avg_2, id) = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT store_id, id
    FROM deduped
    EXCEPT
    SELECT store_id, id
    FROM store_sales
    WHERE product_id IN (3, 7, 10)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN kept_keys k ON s.store_id = k.store_id AND s.id = k.id
),
summary AS (
    -- Multi-level aggregation
    SELECT
        store_id,
        CASE WHEN units_rank_avg_3 >= 8 THEN 'LARGE' ELSE 'SMALL' END AS units_rank_avg_3_tier,
        ROUND(AVG(units_rank), 2) AS avg_units_rank,
        COUNT(*) AS num_rows,
        MAX(units_rank_avg_3) AS max_units_rank_avg_3,
        GROUPING(store_id) AS gid
    FROM verified
    GROUP BY GROUPING SETS ((store_id, CASE WHEN units_rank_avg_3 >= 8 THEN 'LARGE' ELSE 'SMALL' END), (store_id), ())
)
SELECT
    units_rank_avg_3_tier,
    avg_units_rank,
    num_rows,
    gid
FROM summary
ORDER BY num_rows DESC NULLS FIRST, units_rank_avg_3_tier;
