WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share,
        SUM(units) OVER w_recent AS cumulative_units,
        AVG(units) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS units_avg_5,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS revenue_avg_6_prior
    FROM store_sales
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY sale_date) AS latest_rank
        FROM with_windows
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(cumulative_units) OVER w_recent AS prev_cumulative_units,
        LEAD(units_avg_5) OVER w_recent AS next_units_avg_5,
        ROW_NUMBER() OVER w_recent AS event_seq,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC) AS revenue_dense_rank
    FROM ranked
    WHERE units_share IS NOT NULL
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY sale_date, id)
),
ranked_store_sales AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units_avg_5 NULLS LAST, id) AS rn
    FROM enriched
    QUALIFY rn = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.*,
        p.region,
        p.city
    FROM ranked_store_sales src
    LEFT JOIN stores p
        ON src.store_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    region,
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(next_units_avg_5, ARRAY(0.25, 0.5, 0.75)) AS next_units_avg_5_quartiles,
    ROUND(AVG(revenue_avg_6_prior), 2) AS avg_revenue_avg_6_prior,
    ROUND(AVG(units_share), 2) AS avg_units_share,
    MAX(prev_cumulative_units) FILTER (WHERE city = 'New York') AS new_york_max_prev_cumulative_units,
    COUNT(DISTINCT product_id) AS distinct_product_ids
FROM joined
GROUP BY region, DATE_TRUNC('QUARTER', sale_date)
ORDER BY region DESC NULLS FIRST;
