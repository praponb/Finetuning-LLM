WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        CASE WHEN channel = 'store' THEN 1 ELSE 0 END AS is_store
    FROM promotions
    WHERE starts_on >= DATE '2023-08-02'
      AND ends_on <= '2024-04-01'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(is_store) OVER (PARTITION BY channel ORDER BY is_store, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS is_store_avg_2,
        LAG(is_store, 2) OVER (PARTITION BY channel ORDER BY starts_on, id) AS prev_is_store,
        SUM(is_store) OVER (ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_store
    FROM cleaned
    WHERE ends_on >= '2023-07-01'
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM metrics s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL '7' DAY
),
remaining_keys AS (
    -- Set difference on keys
    SELECT promo_code
    FROM attributed
    EXCEPT DISTINCT
    SELECT promo_code
    FROM promotions
    WHERE discount_pct >= 0.23
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM attributed s
    INNER JOIN remaining_keys k ON s.promo_code = k.promo_code
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        ends_on,
        is_store,
        discount_pct,
        AVG(cumulative_is_store) OVER (PARTITION BY channel) AS channel_avg_cumulative_is_store,
        AVG(is_store) OVER (PARTITION BY channel) AS channel_avg_is_store,
        RANK() OVER (ORDER BY promo_id DESC NULLS LAST) AS promo_id_rank
    FROM kept_rows
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM scored a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on < pr.ends_on + INTERVAL '7' DAY
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct_2
    FROM with_window a
    INNER JOIN promotions pr
        ON a.channel = pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on < pr.ends_on + INTERVAL '14' DAY
)
SELECT
    channel,
    ends_on,
    is_store,
    discount_pct,
    channel_avg_cumulative_is_store,
    channel_avg_is_store,
    promo_id_rank,
    promo_id,
    promo_code
FROM matched
ORDER BY ends_on, promo_id;
