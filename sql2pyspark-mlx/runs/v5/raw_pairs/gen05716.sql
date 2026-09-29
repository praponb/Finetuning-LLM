WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        WEEKOFYEAR(sale_date) AS sale_date_weekofyear,
        ADD_MONTHS(sale_date, 3) AS sale_date_plus_months
    FROM store_sales
    WHERE sale_date > DATE '2023-04-13'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS event_seq,
        SUM(units) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units
    FROM filtered
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        sale_date_weekofyear,
        sale_date_plus_months,
        event_seq,
        running_units,
        ROUND(event_seq, 0) AS event_seq_rounded
    FROM with_windows
    WHERE units != 36
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date, id) = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        sale_date,
        units,
        sale_date_weekofyear,
        event_seq,
        DATEDIFF(CURRENT_DATE(), sale_date_plus_months) AS days_since_sale_date_plus_months,
        CAST(sale_date_weekofyear AS STRING) AS sale_date_weekofyear_str
    FROM ranked
    WHERE MONTH(sale_date) IN (3, 4, 5)
),
with_dims AS (
    -- Attach stores attributes
    SELECT s.*, p.city
    FROM cleaned s
    INNER JOIN stores p
        ON s.store_id = p.id
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.*,
        d.opened_date,
        d.city AS store_city
    FROM with_dims src
    LEFT JOIN stores d
        ON src.store_id = d.id
)
SELECT
    s.id,
    s.store_id,
    s.sale_date,
    s.days_since_sale_date_plus_months,
    s.sale_date_weekofyear_str,
    (SELECT SUM(x.amount) FROM orders x WHERE x.store_id = s.store_id) AS sum_order_amount
FROM joined s
