WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        ADD_MONTHS(ends_on, 3) AS ends_on_plus_months,
        ROUND(discount_pct * 1.15, 2) AS discount_pct_adjusted
    FROM promotions
    WHERE starts_on > TO_DATE('2023-04-11')
      AND channel IS NOT NULL
      AND ends_on <= '2024-10-01'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(discount_pct_adjusted, 2) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct_adjusted, id) AS next_discount_pct_adjusted,
        AVG(discount_pct_adjusted) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS discount_pct_adjusted_avg_5
    FROM cleaned
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        ends_on_plus_months,
        discount_pct_adjusted,
        next_discount_pct_adjusted,
        discount_pct_adjusted_avg_5,
        ROW_NUMBER() OVER (ORDER BY starts_on, id) AS event_seq,
        LAST_VALUE(next_discount_pct_adjusted) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY discount_pct_adjusted_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_discount_pct_adjusted,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct_adjusted_avg_5, id) AS seq_no,
        PERCENT_RANK() OVER (PARTITION BY promo_code ORDER BY next_discount_pct_adjusted NULLS FIRST) AS next_discount_pct_adjusted_percent_rank
    FROM with_windows
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct_adjusted DESC, id) AS rn
    FROM scored
    QUALIFY rn = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM deduped a
    JOIN promotions pr
        ON COALESCE(a.channel, 'store') = pr.channel
       AND a.ends_on_plus_months >= pr.starts_on
       AND a.ends_on_plus_months <= DATE_ADD(pr.ends_on, 3)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY last_known_next_discount_pct_adjusted DESC NULLS LAST, id, promo_id) AS latest_rank
        FROM attributed
    ) x
    WHERE latest_rank = 1
)
-- Running and moving aggregates
SELECT
    *,
    LAG(seq_no, 1) OVER (PARTITION BY promotion_promo_code ORDER BY seq_no, id, promo_id) AS prev_seq_no,
    SUM(last_known_next_discount_pct_adjusted) OVER (PARTITION BY promo_code ORDER BY seq_no, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last_known_next_discount_pct_adjusted
FROM ranked
