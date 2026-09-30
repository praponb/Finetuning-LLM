WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, revenue * units AS revenue_per_unit
    FROM store_sales
    WHERE sale_date <= '2025-06-01'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(revenue_per_unit) OVER (ORDER BY sale_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue_per_unit,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id), 4) AS revenue_share,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY revenue_per_unit DESC NULLS FIRST) AS revenue_per_unit_percent_rank
    FROM valid
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        revenue_per_unit,
        cumulative_revenue_per_unit,
        NTILE(5) OVER (ORDER BY revenue ASC, id ASC) AS revenue_bucket,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue_per_unit_percent_rank ASC, id ASC) AS seq_no,
        AVG(revenue_share) OVER (PARTITION BY store_id ORDER BY cumulative_revenue_per_unit ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS revenue_share_avg_3
    FROM with_windows
    WHERE revenue_share >= 0.21
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.units,
        e.revenue,
        e.cumulative_revenue_per_unit,
        e.revenue_bucket,
        e.seq_no,
        e.revenue_share_avg_3,
        d.brand,
        d.category,
        d.stock_qty
    FROM metrics e
    LEFT JOIN products d
        ON e.product_id = d.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (ORDER BY seq_no ASC, id ASC) AS seq_no_bucket,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue_share_avg_3 DESC, id ASC) AS row_seq
    FROM enriched
)
-- Window metrics over several different partitions
SELECT
    id,
    units,
    revenue,
    cumulative_revenue_per_unit,
    seq_no,
    revenue_share_avg_3,
    category,
    stock_qty,
    seq_no_bucket,
    row_seq,
    SUM(revenue_share_avg_3) OVER (PARTITION BY product_id ORDER BY units ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue_share_avg_3,
    AVG(revenue_bucket) OVER (PARTITION BY store_id ORDER BY cumulative_revenue_per_unit ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_bucket_avg_3_prior
FROM scored;
