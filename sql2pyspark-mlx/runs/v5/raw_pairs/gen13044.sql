WITH base AS (
    SELECT
        *,
        CONCAT_WS(',', city, supplier_name) AS city_supplier_name_key,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM suppliers
    WHERE city IS NOT NULL
),
with_latest AS (
    SELECT
        s.*,
        top1.category,
        top1.brand
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.category, c.brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
matched_keys AS (
    SELECT city, country
    FROM with_latest
    EXCEPT
    SELECT city, country
    FROM suppliers
    WHERE country NOT IN ('TH', 'UK')
),
reconciled AS (
    SELECT s.*
    FROM with_latest s
    JOIN matched_keys k ON s.city = k.city AND s.country = k.country
),
enriched AS (
    SELECT
        country,
        city_supplier_name_key,
        brand,
        RANK() OVER (PARTITION BY category, brand ORDER BY rating DESC NULLS FIRST) AS rating_rank,
        RANK() OVER (PARTITION BY city ORDER BY rating DESC NULLS FIRST) AS rating_rank_2,
        AVG(rating) OVER (PARTITION BY country) AS country_avg_rating
    FROM reconciled
    WHERE category IS NOT NULL
)
SELECT
    brand,
    rating_rank,
    rating_rank_2,
    STDDEV_SAMP(rating_rank_2) OVER w_grp AS country_stddev_samp_rating_rank_2
FROM enriched
WINDOW w_grp AS (PARTITION BY country, brand);
