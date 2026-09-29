WITH scored AS (
    SELECT
        id,
        supplier_name,
        city,
        rating,
        LAG(rating, 1, 0) OVER w_grp AS prev_rating,
        AVG(rating) OVER (PARTITION BY country, city) AS country_avg_rating,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        AVG(rating) OVER (PARTITION BY country ORDER BY rating ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS rating_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id ASC) AS event_seq
    FROM suppliers
    WHERE city IN ('Berlin', 'Bangkok', 'London')
    WINDOW w_grp AS (ORDER BY rating ASC, id ASC)
)
SELECT
    id,
    supplier_name,
    city,
    rating,
    prev_rating,
    rating_avg_5_prior
FROM scored
ORDER BY id DESC NULLS LAST
