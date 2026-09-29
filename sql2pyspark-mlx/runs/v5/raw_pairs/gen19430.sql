WITH valid AS (
    SELECT *, DATE_ADD(sale_date, 53) AS sale_date_plus_53d
    FROM store_sales
    WHERE units IS NULL
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY sale_date_plus_53d DESC NULLS FIRST) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
scored AS (
    SELECT
        *,
        CORR(units, revenue) OVER (PARTITION BY store_id) AS corr_units_revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS row_seq,
        SUM(units) OVER (PARTITION BY product_id ORDER BY sale_date_plus_53d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units
    FROM latest
),
enriched AS (
    SELECT
        s.*,
        p2.region,
        p2.opened_date,
        p2.sqft
    FROM scored s
    INNER JOIN stores p2
        ON s.store_id = p2.id
)
SELECT
    id,
    product_id,
    sale_date,
    units,
    revenue,
    sale_date_plus_53d,
    row_seq,
    running_units,
    region,
    CASE WHEN region RLIKE '^(West|Central)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS region_zone
FROM enriched
ORDER BY running_units DESC NULLS FIRST
