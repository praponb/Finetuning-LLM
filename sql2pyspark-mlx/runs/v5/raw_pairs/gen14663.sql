WITH cleaned AS (
    SELECT *, ROUND(revenue, 0) AS revenue_rounded
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-05-22')
      AND store_id = 2
),
with_windows AS (
    SELECT
        id,
        store_id,
        product_id,
        revenue_rounded,
        LAG(revenue, 1, 0) OVER (PARTITION BY product_id ORDER BY revenue, id) AS prev_revenue,
        LEAD(units, 1) OVER (PARTITION BY product_id, store_id ORDER BY units, id) AS next_units
    FROM cleaned
),
with_dims AS (
    SELECT
        a.*,
        p.opened_date,
        p.city,
        p.store_name
    FROM with_windows a
    INNER JOIN stores p
        ON a.store_id = p.id
),
ranked AS (
    SELECT
        id,
        product_id,
        revenue_rounded,
        next_units,
        opened_date
    FROM with_dims
    QUALIFY RANK() OVER (PARTITION BY store_id, product_id ORDER BY revenue_rounded DESC NULLS LAST) = 1
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_recent AS row_seq,
        SUM(next_units) OVER (PARTITION BY product_id ORDER BY revenue_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_units,
        LEAD(next_units, 1) OVER (ORDER BY revenue_rounded, id) AS next_next_units
    FROM ranked
    WINDOW w_recent AS (ORDER BY next_units DESC, id)
)
SELECT
    d.brand,
    d.category,
    COUNT(*) AS num_rows,
    ROUND(SUM(a.next_units), 2) AS total_next_units,
    MAX(a.revenue_rounded) AS max_revenue_rounded,
    GROUPING_ID() AS level_id
FROM scored a
INNER JOIN products d ON a.product_id = d.id
GROUP BY GROUPING SETS ((d.brand, d.category), (d.brand))
ORDER BY brand NULLS FIRST;
