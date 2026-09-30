WITH latest AS (
    SELECT *
    FROM store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id ASC) = 1
),
base AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM latest
    WHERE sale_date > '2025-04-22'
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC NULLS FIRST, id ASC) AS rank_in_group
        FROM base
    ) ranked
    WHERE rank_in_group = 1
)
SELECT
    id,
    store_id,
    sale_date,
    units,
    revenue,
    rank_in_group
FROM deduped;
