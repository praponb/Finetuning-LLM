WITH valid AS (
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        ROUND(revenue, 0) AS revenue_rounded
    FROM store_sales
    WHERE store_id = 6
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC NULLS FIRST, id) AS dedup_rank
        FROM valid
    ) t
    WHERE dedup_rank = 1
),
with_dims AS (
    SELECT s.*, p.launch_date
    FROM deduped s
    LEFT JOIN products p
        ON s.product_id = p.id
    WHERE p.launch_date <= '2023-04-05'
),
joined AS (
    SELECT
        a.id,
        a.product_id,
        a.units,
        a.revenue_rounded,
        a.launch_date,
        d.product_name,
        d.stock_qty
    FROM with_dims a
    LEFT JOIN products d
        ON a.product_id = d.id
),
stats AS (
    SELECT
        DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS num_rows,
        ROUND(AVG(units), 2) AS avg_units,
        COUNT(*) AS cnt,
        BIT_OR(units) AS units_bit_or
    FROM joined
    GROUP BY DATE_TRUNC('QUARTER', launch_date)
)
SELECT
    quarter_start,
    distinct_ids,
    num_rows,
    avg_units,
    cnt,
    units_bit_or
FROM stats
ORDER BY distinct_ids DESC NULLS FIRST
