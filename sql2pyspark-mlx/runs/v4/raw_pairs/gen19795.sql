WITH cleaned AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM store_sales
    WHERE sale_date >= TO_DATE('2024-02-16')
),
enriched AS (
    SELECT
        *,
        CORR(revenue, units) OVER (PARTITION BY store_id) AS corr_revenue_units,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share
    FROM cleaned
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY corr_revenue_units DESC NULLS FIRST) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
latest_store_sales AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY store_id, product_id ORDER BY revenue NULLS FIRST) AS row_num
        FROM latest
    ) x
    WHERE row_num = 1
),
valid AS (
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        days_since_sale_date,
        corr_revenue_units,
        units_share,
        row_num,
        DATE_TRUNC('week', sale_date) AS sale_date_period
    FROM latest_store_sales
    WHERE rank_in_group != 1
),
metrics AS (
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        days_since_sale_date,
        corr_revenue_units,
        units_share,
        row_num,
        sale_date_period,
        FIRST_VALUE(units_share) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_units_share,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY units DESC) AS units_percent_rank,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY sale_date_period, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_avg_3_prior
    FROM valid
),
grouped AS (
    SELECT
        store_id,
        DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
        (UNIX_TIMESTAMP(MAX(sale_date_period)) - UNIX_TIMESTAMP(MIN(sale_date_period))) / 60 AS span_minutes,
        ROUND(AVG(corr_revenue_units), 2) AS avg_corr_revenue_units,
        (UNIX_TIMESTAMP(MAX(sale_date_period)) - UNIX_TIMESTAMP(MIN(sale_date_period))) / 60 AS span_minutes_2,
        APPROX_PERCENTILE(units_percent_rank, 0.5) AS units_percent_rank_p50,
        BIT_AND(units) AS units_bit_and,
        (UNIX_TIMESTAMP(MAX(sale_date_period)) - UNIX_TIMESTAMP(MIN(sale_date_period))) / 60 AS span_minutes_3,
        GROUPING(store_id) AS grouping_id
    FROM metrics
    GROUP BY ROLLUP (store_id, DATE_TRUNC('QUARTER', sale_date))
    HAVING (UNIX_TIMESTAMP(MAX(sale_date_period)) - UNIX_TIMESTAMP(MIN(sale_date_period))) / 60 > 0
)
SELECT
    s.store_id,
    s.span_minutes,
    s.avg_corr_revenue_units,
    s.units_percent_rank_p50,
    s.units_bit_and,
    s.span_minutes_3,
    s.grouping_id,
    (SELECT AVG(x.revenue) FROM store_sales x WHERE x.store_id = s.store_id) AS avg_store_sale_revenue
FROM grouped s
ORDER BY span_minutes_3 DESC
