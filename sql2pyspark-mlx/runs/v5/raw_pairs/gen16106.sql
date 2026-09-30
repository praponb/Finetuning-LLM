WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        DATE_TRUNC('month', sale_date) AS sale_date_period
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-03-31')
      AND YEAR(sale_date) = 2024
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(units) OVER w_recent AS running_sum_units,
        MIN(units) OVER w_recent AS running_min_units,
        COUNT(units) OVER w_recent AS running_count_units,
        AVG(units) OVER w_recent AS running_avg_units
    FROM valid
    WHERE revenue > 2700
    WINDOW w_recent AS (PARTITION BY store_id, product_id ORDER BY sale_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC NULLS LAST, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM deduped
    EXCEPT
    SELECT product_id, id
    FROM store_sales
    WHERE units > 16
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, running_count_units * running_min_units AS running_count_units_times_running_min_units
    FROM kept_rows
    WHERE sale_date > DATE '2023-04-27'
      AND store_id IN (1, 2, 5, 6)
)
SELECT
    store_id,
    product_id,
    sale_date,
    sale_date_period,
    running_sum_units,
    running_count_units,
    running_avg_units,
    rank_in_group,
    running_count_units_times_running_min_units
FROM base
ORDER BY store_id, running_avg_units DESC
