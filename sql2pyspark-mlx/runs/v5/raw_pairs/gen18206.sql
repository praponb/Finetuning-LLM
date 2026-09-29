WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER w_running AS units_bucket,
        AVG(units) OVER w_hist AS units_avg_6_prior
    FROM store_sales
    WINDOW w_running AS (PARTITION BY store_id ORDER BY units DESC, id), w_hist AS (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DAY(sale_date) AS sale_date_day
    FROM scored
    WHERE sale_date > DATE '2023-07-31'
      AND sale_date <= '2025-10-01'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date_day, id) AS rank_in_group
        FROM filtered
    ) ranked
    WHERE rank_in_group = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(units_bucket / SUM(units_bucket) OVER (PARTITION BY product_id), 4) AS units_bucket_share,
        STDDEV_SAMP(units) OVER (PARTITION BY store_id, product_id) AS store_id_stddev_samp_units,
        NTILE(10) OVER (PARTITION BY store_id ORDER BY units_bucket, id) AS units_bucket_bucket,
        SUM(units_avg_6_prior) OVER (PARTITION BY product_id, store_id ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units_avg_6_prior,
        AVG(units_bucket) OVER (PARTITION BY product_id ORDER BY units_bucket, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS units_bucket_avg_3
    FROM latest
),
enriched_store_sales AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(units_bucket_bucket) OVER w_running AS prev_units_bucket_bucket,
        LAG(sale_date_day) OVER w_running AS prev_sale_date_day,
        LEAD(sale_date_day) OVER w_running AS next_sale_date_day,
        ROW_NUMBER() OVER w_running AS event_seq,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM enriched
    WHERE store_id_stddev_samp_units <> 37
    WINDOW w_running AS (PARTITION BY store_id, product_id ORDER BY units, id), w_ordered AS (PARTITION BY product_id ORDER BY revenue, id)
)
SELECT
    id,
    store_id,
    product_id,
    sale_date,
    units,
    units_bucket,
    units_avg_6_prior,
    sale_date_day,
    units_bucket_share
FROM enriched_store_sales
ORDER BY product_id DESC;
