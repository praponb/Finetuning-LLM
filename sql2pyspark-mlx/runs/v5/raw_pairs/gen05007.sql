WITH exploded AS (
    SELECT
        base.id,
        base.customer_id,
        base.created_at,
        pos + 1 AS line_no,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts base
    LATERAL VIEW POSEXPLODE(base.items) li AS pos, elem
),
flagged AS (
    SELECT *, CASE WHEN LAG(created_at) OVER (PARTITION BY customer_id ORDER BY created_at, id, line_no) IS NULL
                 OR created_at > LAG(created_at) OVER (PARTITION BY customer_id ORDER BY created_at, id, line_no) + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM exploded
),
sessions_carts AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY created_at, id, line_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
metrics AS (
    SELECT
        *,
        SUM(price) OVER (PARTITION BY customer_id ORDER BY product_id, id, line_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price,
        AVG(island_id) OVER (PARTITION BY customer_id ORDER BY island_id, id, line_no ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS island_id_avg_6
    FROM sessions_carts
),
enriched AS (
    SELECT
        s.*,
        p.email,
        p.signup_date,
        p.country
    FROM metrics s
    INNER JOIN customers p
        ON s.customer_id = p.id
),
matched AS (
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM enriched a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.created_at BETWEEN sub.start_date AND sub.end_date
),
enriched_carts AS (
    SELECT
        id,
        customer_id,
        created_at,
        line_no,
        product_id,
        qty,
        promo,
        island_id,
        email,
        signup_date,
        country,
        subscription_id,
        PERCENT_RANK() OVER w_grp AS monthly_fee_percent_rank,
        MIN(price) OVER (PARTITION BY plan, promo) AS plan_min_price
    FROM matched
    WINDOW w_grp AS (PARTITION BY country ORDER BY monthly_fee DESC NULLS LAST)
),
gaps AS (
    SELECT *, CASE WHEN created_at - LAG(created_at) OVER (PARTITION BY promo ORDER BY created_at, id, line_no, subscription_id) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM enriched_carts
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY promo ORDER BY created_at, id, line_no, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM gaps
)
SELECT
    s.id,
    s.customer_id,
    s.created_at,
    s.line_no,
    s.product_id,
    s.promo,
    s.island_id,
    s.email,
    s.country,
    (SELECT SUM(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS sum_account_balance
FROM sessionized s;
