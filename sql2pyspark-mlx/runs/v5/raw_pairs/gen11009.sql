WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date_2
    FROM store_sales
    WHERE store_id IN (2, 3, 4)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY days_since_sale_date_2, id) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY revenue DESC, id) AS row_seq_2,
        LAG(units, 2, 0) OVER (ORDER BY days_since_sale_date_2, id) AS prev_units,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY days_since_sale_date NULLS LAST) AS days_since_sale_date_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units, id) AS event_seq
    FROM filtered
    WHERE days_since_sale_date_2 > 73
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(row_seq, days_since_sale_date) AS greatest_row_seq_days_since_sale_date
    FROM enriched
    WHERE sale_date >= DATE '2023-06-19'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY days_since_sale_date_2 DESC) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
totals AS (
    -- Multi-level aggregation
    SELECT
        store_id,
        SUM(days_since_sale_date) AS total_days_since_sale_date,
        COUNT(DISTINCT id) AS distinct_ids
    FROM ranked
    GROUP BY ROLLUP (store_id)
)
SELECT
    s.store_id,
    s.total_days_since_sale_date,
    s.distinct_ids,
    (SELECT AVG(x.quantity) FROM inventory x WHERE x.store_id = s.store_id) AS avg_inventory_quantity
FROM totals s
ORDER BY avg_inventory_quantity DESC NULLS FIRST, distinct_ids
