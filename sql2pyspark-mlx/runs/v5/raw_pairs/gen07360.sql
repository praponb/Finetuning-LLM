WITH flattened AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS key, v
),
matched_keys AS (
    SELECT customer_id, id
    FROM flattened
    EXCEPT DISTINCT
    SELECT customer_id, id
    FROM carts
    WHERE created_at >= '2025-10-01'
),
verified AS (
    SELECT s.*
    FROM flattened s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
joined AS (
    SELECT
        src.*,
        d.name,
        d.credit_limit,
        d.age
    FROM verified src
    INNER JOIN customers d
        ON src.customer_id <=> d.id
),
joined_carts AS (
    SELECT
        a.id,
        a.customer_id,
        a.coupon_codes,
        a.key,
        a.v,
        a.age,
        c.name AS customer_name
    FROM joined a
    INNER JOIN customers c
        ON a.customer_id = c.id
    WHERE c.email IS NOT NULL
),
remaining_keys AS (
    SELECT customer_id, id
    FROM joined_carts
    INTERSECT ALL
    SELECT customer_id, id
    FROM carts
    WHERE created_at < '2024-11-03'
),
reconciled AS (
    SELECT s.*
    FROM joined_carts s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.id = k.id
)
SELECT
    customer_id,
    coupon_codes,
    key,
    v,
    age,
    customer_name,
    STDDEV_SAMP(age) OVER (PARTITION BY key) AS key_stddev_samp_age,
    STDDEV_SAMP(age) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_age,
    RANK() OVER (PARTITION BY key ORDER BY age DESC NULLS FIRST) AS age_rank,
    PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY age ASC) AS age_percent_rank
FROM reconciled;
