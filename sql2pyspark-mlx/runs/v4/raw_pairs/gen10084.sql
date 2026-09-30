WITH joined AS (
    SELECT
        a.*,
        d.region,
        d.store_name,
        d.opened_date
    FROM store_sales a
    LEFT JOIN stores d
        ON a.store_id = d.id
),
base AS (
    SELECT
        *,
        ROUND(revenue * 0.9, 2) AS revenue_adjusted,
        MONTH(sale_date) AS sale_date_month,
        COALESCE(region, 'OTHER') AS resolved_region
    FROM joined
    WHERE region IS NOT NULL
),
metrics AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        region,
        store_name,
        opened_date,
        revenue_adjusted,
        sale_date_month,
        resolved_region,
        RANK() OVER (PARTITION BY store_id ORDER BY revenue_adjusted DESC NULLS LAST) AS revenue_adjusted_rank,
        LEAD(sale_date_month, 1) OVER (PARTITION BY product_id ORDER BY units ASC, id ASC) AS next_sale_date_month
    FROM base
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue ASC NULLS FIRST, id ASC) AS rn
    FROM metrics
    QUALIFY rn = 1
),
latest_store_sales AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY sale_date_month ASC NULLS LAST, id ASC) AS rank_in_group
        FROM latest
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
    SELECT
        *,
        LAST_VALUE(next_sale_date_month, TRUE) OVER w_hist AS last_known_next_sale_date_month,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY next_sale_date_month DESC NULLS LAST) AS next_sale_date_month_dense_rank
    FROM latest_store_sales
    WINDOW w_hist AS (PARTITION BY region ORDER BY sale_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        region,
        store_name,
        opened_date,
        revenue_adjusted,
        sale_date_month,
        resolved_region,
        revenue_adjusted_rank,
        next_sale_date_month,
        rn,
        rank_in_group,
        last_known_next_sale_date_month,
        next_sale_date_month_dense_rank,
        ROW_NUMBER() OVER (ORDER BY sale_date DESC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY sale_date_month ASC, id ASC) AS row_seq,
        LEAD(last_known_next_sale_date_month, 1, 0) OVER (ORDER BY units ASC, id ASC) AS next_last_known_next_sale_date_month,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY sale_date_month DESC NULLS LAST) AS sale_date_month_cume_dist,
        AVG(revenue_adjusted) OVER (PARTITION BY store_id ORDER BY revenue_adjusted_rank ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS revenue_adjusted_avg_5
    FROM with_windows
)
SELECT
    id,
    store_id,
    product_id,
    sale_date,
    revenue,
    region,
    opened_date,
    revenue_adjusted,
    sale_date_month
FROM enriched
ORDER BY product_id DESC, sale_date_month DESC
