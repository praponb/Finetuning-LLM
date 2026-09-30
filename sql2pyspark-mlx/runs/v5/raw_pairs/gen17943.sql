WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.price >= 340
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(rating) OVER w_grp cumulative_rating,
        NTILE(10) OVER w_part rating_bucket,
        AVG(rating) OVER w_hist rating_avg_6_prior,
        AVG(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) rating_avg_3_prior,
        ROW_NUMBER() OVER (ORDER BY rating, id) row_seq
    FROM not_flagged
    WINDOW w_grp AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY country ORDER BY rating, id), w_hist AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.rating,
        s.row_seq,
        x.product_brand
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('/', product_brand, country) product_brand_country_key,
        COALESCE(row_seq, 0) row_seq_filled
    FROM latest_match
    WHERE product_brand IS NOT NULL
      AND product_brand IS DISTINCT FROM 'Umbrella'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY product_brand ORDER BY row_seq DESC, id) row_seq_bucket,
        LAST_VALUE(row_seq_filled) IGNORE NULLS OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_row_seq_filled,
        SUM(rating) OVER (PARTITION BY product_brand, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_rating,
        FIRST_VALUE(rating) IGNORE NULLS OVER (PARTITION BY country ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_rating,
        LAST_VALUE(row_seq_filled, TRUE) OVER w_running last_known_row_seq_filled_2
    FROM filtered
    WINDOW w_running AS (PARTITION BY country, product_brand ORDER BY row_seq_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    country,
    rating,
    row_seq,
    product_brand,
    last_known_row_seq_filled,
    cumulative_rating,
    CASE WHEN product_brand RLIKE '^(Initech|Umbrella)$' THEN 'EXTENDED' WHEN product_brand RLIKE '^(Acme|Globex)$' THEN 'CORE' ELSE 'PRIORITY' END AS product_brand_zone
FROM with_windows
ORDER BY product_brand DESC, row_seq
