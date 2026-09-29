WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        price,
        cost,
        supplier_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price, id) AS latest_rank
        FROM products
    ) t
    WHERE latest_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(cost) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_cost,
        AVG(price) OVER w_running price_avg_3_prior
    FROM ranked
    WINDOW w_running AS (PARTITION BY supplier_id ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        cost,
        supplier_id,
        price_avg_3_prior,
        SUM(cost) OVER (PARTITION BY supplier_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_cost,
        ROUND(price_avg_3_prior / SUM(price_avg_3_prior) OVER (PARTITION BY supplier_id), 4) price_avg_3_prior_share
    FROM metrics
    WHERE price < 780
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.cost,
        a.supplier_id,
        a.running_cost,
        b.city,
        b.rating,
        b.country
    FROM scored a
    INNER JOIN suppliers b
        ON a.supplier_id = b.id
)
-- Window metrics over several different partitions
SELECT
    id,
    cost,
    supplier_id,
    country,
    LAG(rating) OVER (PARTITION BY country ORDER BY running_cost, id) prev_rating,
    LEAD(rating) OVER (PARTITION BY country ORDER BY running_cost, id) next_rating,
    LEAD(running_cost) OVER (PARTITION BY country ORDER BY running_cost, id) next_running_cost,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY running_cost, id) event_seq
FROM with_dims;
