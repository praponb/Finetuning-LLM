WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        revenue,
        CASE WHEN revenue >= 4000 THEN 'High' WHEN revenue >= 1100 THEN 'Medium' ELSE 'Low' END AS revenue_band
    FROM store_sales
    WHERE sale_date > DATE '2023-09-20'
),
joined AS (
    -- Attach stores attributes
    SELECT
        a.*,
        b.sqft,
        b.city
    FROM filtered a
    INNER JOIN stores b
        ON a.store_id = b.id
),
joined_store_sales AS (
    -- Attach stores attributes
    SELECT
        a.*,
        s.store_name,
        s.sqft AS store_sqft
    FROM joined a
    INNER JOIN stores s
        ON a.store_id = s.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY store_id, city ORDER BY sqft DESC) AS latest_rank
    FROM joined_store_sales
    QUALIFY latest_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        revenue,
        sqft,
        latest_rank,
        AVG(store_sqft) OVER (ORDER BY latest_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS store_sqft_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY revenue, id) AS event_seq
    FROM latest
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN sqft >= 15347 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
        PERCENTILE_APPROX(event_seq, 0.9) AS event_seq_p90,
        ROUND(AVG(revenue), 2) AS avg_revenue
    FROM with_windows
    GROUP BY CASE WHEN sqft >= 15347 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    sqft_tier,
    event_seq_p90,
    avg_revenue
FROM stats
