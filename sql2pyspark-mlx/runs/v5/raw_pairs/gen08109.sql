WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(revenue, 2) revenue_rounded,
        DATE_FORMAT(sale_date, 'yyyy-MM') sale_date_label
    FROM store_sales
    WHERE sale_date > DATE '2023-08-24'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY sale_date DESC NULLS FIRST, id) AS dedup_rank
        FROM cleaned
    ) ranked
    WHERE dedup_rank = 1
),
ranked_store_sales AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY revenue DESC, id) AS rank_in_group
        FROM ranked
    ) t
    WHERE rank_in_group = 1
),
ranked_store_sales_2 AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        product_id,
        sale_date,
        sale_date_label,
        dedup_rank
    FROM ranked_store_sales
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY rank_in_group DESC) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) event_seq,
        ROW_NUMBER() OVER (ORDER BY sale_date, id) event_seq_2,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) row_seq,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY dedup_rank, id) event_seq_3
    FROM ranked_store_sales_2
    WHERE product_id IN (1, 6)
),
enriched_store_sales AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.sale_date,
        s.sale_date_label,
        s.dedup_rank,
        s.event_seq_2,
        s.row_seq,
        s.event_seq_3,
        d.launch_date,
        d.price
    FROM enriched s
    LEFT JOIN products d
        ON s.product_id = d.id
),
enriched_store_sales_2 AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.sale_date_label,
        a.dedup_rank,
        a.event_seq_2,
        a.launch_date,
        a.price,
        p.launch_date AS product_launch_date,
        p.category
    FROM enriched_store_sales a
    LEFT JOIN products p
        ON a.product_id = p.id
)
-- Multi-level aggregation
SELECT
    product_id,
    MIN(price) AS min_price,
    COUNT_IF(category = 'Electronics') AS electronics_events,
    COUNT(CASE WHEN category = 'Home' THEN 1 END) AS home_count,
    SUM(CASE WHEN category = 'Home' THEN price ELSE 0 END) AS home_price,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    MAX(event_seq_2) FILTER (WHERE category = 'Sports') AS sports_max_event_seq_2
FROM enriched_store_sales_2
GROUP BY 1
ORDER BY product_id NULLS FIRST;
