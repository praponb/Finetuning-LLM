WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        item.qty,
        item.price
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS item
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        coupon_codes,
        qty,
        price,
        SUM(qty) OVER (PARTITION BY customer_id) AS customer_id_sum_qty,
        ROUND(price / SUM(price) OVER (PARTITION BY customer_id), 4) AS price_share,
        ROUND(qty / SUM(qty) OVER (PARTITION BY customer_id), 4) AS qty_share,
        RANK() OVER (PARTITION BY customer_id ORDER BY qty) AS qty_rank
    FROM flattened
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT customer_id
    FROM scored
    INTERSECT ALL
    SELECT customer_id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id
),
scored_carts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY price) AS price_percent_rank,
        MAX(price_share) OVER (PARTITION BY customer_id) AS customer_id_max_price_share
    FROM reconciled
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        qty,
        customer_id_sum_qty,
        price_share,
        qty_share,
        qty_rank,
        customer_id_max_price_share,
        ROUND(qty, 2) AS qty_rounded
    FROM scored_carts
    WHERE SIZE(coupon_codes) >= 2
),
scored_carts_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        qty,
        customer_id_sum_qty,
        price_share,
        qty_share,
        qty_rank,
        customer_id_max_price_share,
        qty_rounded,
        RANK() OVER (PARTITION BY customer_id ORDER BY customer_id_max_price_share DESC NULLS LAST) AS customer_id_max_price_share_rank,
        PERCENT_RANK() OVER (ORDER BY qty_rank DESC NULLS FIRST) AS qty_rank_percent_rank,
        CUME_DIST() OVER (ORDER BY customer_id_sum_qty DESC NULLS FIRST) AS customer_id_sum_qty_cume_dist
    FROM filtered
)
SELECT
    s.id,
    s.customer_id,
    s.customer_id_sum_qty,
    s.price_share,
    s.qty_rank,
    s.customer_id_max_price_share,
    s.qty_rounded,
    s.qty_rank_percent_rank,
    s.customer_id_sum_qty_cume_dist,
    (SELECT SUM(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS sum_account_balance
FROM scored_carts_2 s
ORDER BY customer_id_sum_qty DESC NULLS FIRST, qty_rounded;
