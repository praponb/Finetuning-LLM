WITH enriched AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY country, city ORDER BY rating NULLS FIRST) AS rating_percent_rank,
        COUNT(rating) OVER (PARTITION BY city) AS city_count_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share
    FROM suppliers
    WHERE rating >= 2.7
),
enriched_suppliers AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating_percent_rank,
        rating_share,
        ROW_NUMBER() OVER (ORDER BY city_count_rating DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY city_count_rating, id) AS seq_no_2,
        LAG(rating_percent_rank, 1) OVER (ORDER BY city_count_rating, id) AS prev_rating_percent_rank,
        AVG(rating_share) OVER (PARTITION BY country) AS country_avg_rating_share
    FROM enriched
),
filtered AS (
    SELECT
        id,
        supplier_name,
        rating_share,
        seq_no_2,
        country_avg_rating_share,
        CASE WHEN seq_no >= 7 THEN 'High' WHEN seq_no >= 3 THEN 'Medium' ELSE 'Low' END AS seq_no_tier,
        rating_share + country_avg_rating_share AS rating_share_plus_country_avg_rating_share,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM enriched_suppliers
    WHERE city IS NOT NULL
      AND city IS NOT DISTINCT FROM 'Berlin'
),
enriched_suppliers_2 AS (
    SELECT
        *,
        LEAD(country_avg_rating_share, 1) OVER (ORDER BY country_avg_rating_share, id) AS next_country_avg_rating_share,
        AVG(seq_no_2) OVER (ORDER BY rating_share_plus_country_avg_rating_share, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS seq_no_2_avg_2_prior,
        DENSE_RANK() OVER (ORDER BY seq_no_2 DESC NULLS FIRST) AS seq_no_2_dense_rank
    FROM filtered
),
latest_match AS (
    SELECT
        s.id,
        s.seq_no_tier,
        s.seq_no_2_avg_2_prior,
        s.seq_no_2_dense_rank,
        latest.category
    FROM enriched_suppliers_2 s
    LEFT JOIN LATERAL (
            SELECT c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    seq_no_tier,
    seq_no_2_dense_rank,
    category,
    CASE WHEN category RLIKE '^(Sports|Home)$' THEN 'GROUP_B' WHEN category RLIKE '^(Electronics|Books)$' THEN 'GROUP_A' ELSE 'CORE' END AS category_zone
FROM latest_match
