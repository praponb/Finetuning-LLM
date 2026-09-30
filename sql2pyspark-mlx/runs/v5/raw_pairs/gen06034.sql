WITH valid AS (
    SELECT
        id,
        units,
        revenue,
        ADD_MONTHS(sale_date, 1) AS sale_date_plus_months
    FROM store_sales
    WHERE revenue BETWEEN 2400 AND 2500
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_part AS event_seq,
        NTILE(4) OVER (ORDER BY units, id) AS units_bucket,
        LAST_VALUE(units, TRUE) OVER w_part_2 AS last_known_units
    FROM valid
    WHERE units BETWEEN 14 AND 36
    WINDOW w_part AS (ORDER BY revenue DESC, id), w_part_2 AS (ORDER BY sale_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    SELECT
        *,
        LEAD(revenue) OVER w_grp AS next_revenue,
        LAG(last_known_units) OVER w_grp AS prev_last_known_units,
        ROW_NUMBER() OVER w_grp AS row_seq,
        NTILE(4) OVER (ORDER BY last_known_units DESC, id) AS last_known_units_bucket
    FROM enriched
    WINDOW w_grp AS (ORDER BY sale_date_plus_months, id)
),
metrics AS (
    SELECT
        id,
        revenue,
        sale_date_plus_months,
        event_seq,
        units_bucket,
        last_known_units,
        row_seq,
        ROW_NUMBER() OVER (ORDER BY units_bucket DESC, id) AS event_seq_2,
        LAG(last_known_units_bucket, 1) OVER (ORDER BY units_bucket, id) AS prev_last_known_units_bucket,
        ROW_NUMBER() OVER (ORDER BY units DESC, id) AS event_seq_3
    FROM scored
)
SELECT
    id,
    revenue,
    sale_date_plus_months,
    event_seq,
    units_bucket,
    last_known_units,
    row_seq,
    event_seq_2,
    prev_last_known_units_bucket
FROM metrics
ORDER BY sale_date_plus_months;
