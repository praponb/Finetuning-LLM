WITH base AS (
    SELECT *, DATE_ADD(sale_date, 10) AS sale_date_plus_10d
    FROM store_sales
    WHERE sale_date >= DATE '2023-05-23'
),
with_windows AS (
    SELECT
        id,
        store_id,
        revenue,
        LEAD(units) OVER w_hist AS next_units,
        LEAD(revenue) OVER w_hist AS next_revenue,
        ROW_NUMBER() OVER w_hist AS event_seq,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id, product_id), 4) AS units_share
    FROM base
    WINDOW w_hist AS (PARTITION BY store_id ORDER BY sale_date_plus_10d, id)
),
ranked AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY next_units NULLS FIRST, id) = 1
),
ranked_store_sales AS (
    SELECT
        id,
        next_units,
        units_share
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY event_seq DESC NULLS FIRST, id) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
),
cleaned AS (
    SELECT
        *,
        next_units * units_share AS next_units_times_units_share,
        CAST(units_share AS INT) AS units_share_int
    FROM ranked_store_sales
    WHERE units_share <= 0.53
),
kept_keys AS (
    SELECT id
    FROM cleaned
    EXCEPT
    SELECT id
    FROM store_sales
    WHERE revenue BETWEEN 1900 AND 3300
),
verified AS (
    SELECT s.*
    FROM cleaned s
    JOIN kept_keys k ON s.id = k.id
)
SELECT
    units_share,
    next_units_times_units_share,
    units_share_int
FROM verified
ORDER BY units_share_int, next_units_times_units_share;
