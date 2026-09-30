WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        ROW_NUMBER() OVER w_recent AS row_seq,
        AVG(rating) OVER w_hist AS rating_avg_6,
        LAST_VALUE(city) IGNORE NULLS OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city
    FROM suppliers
    WINDOW w_recent AS (PARTITION BY country ORDER BY rating DESC, id), w_hist AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating, id) AS row_seq_2,
        MAX(row_seq) OVER (PARTITION BY city, country) AS city_max_row_seq
    FROM enriched
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating_avg_6, id) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY running_rating DESC, id) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.brand,
        x.product_launch_date
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.brand, c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date >= s.product_launch_date
        )
),
deduped_suppliers AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM not_flagged
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY running_rating NULLS LAST) = 1
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            brand,
            country,
            rating
        FROM deduped_suppliers
    ) src
    PIVOT (
        SUM(rating) AS amount,
        COUNT(1) AS item_count
        FOR country IN ('US', 'TH', 'UK')
    )
)
SELECT
    brand,
    TH_amount,
    TH_item_count,
    UK_amount,
    CASE WHEN brand RLIKE '^(Stark|Globex)$' THEN 'CORE' WHEN brand RLIKE '^(Acme|Initech)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS brand_zone
FROM wide
ORDER BY TH_item_count NULLS LAST, brand;
