WITH valid AS (
    SELECT *, DATE_SUB(sale_date, 23) AS sale_date_minus_23d
    FROM store_sales
    WHERE revenue < 3900
)
SELECT
    id,
    store_id,
    units,
    ROW_NUMBER() OVER (ORDER BY units DESC, id) AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) AS row_seq
FROM valid;
