WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LAST_DAY(ends_on) AS ends_on_month_end,
        DATE_TRUNC('WEEK', ends_on) AS ends_on_period,
        COALESCE(channel, 'N/A') AS channel_clean
    FROM promotions
    WHERE channel IS NOT NULL
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM base src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.ends_on_month_end BETWEEN pr.starts_on AND pr.ends_on
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM with_window e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL '7' DAY
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(ends_on_period) OVER (PARTITION BY promotion_promo_code_2 ORDER BY ends_on_period ASC, id ASC, promo_id ASC, promo_id_2 ASC) IS NULL
                 OR ends_on_period > LAG(ends_on_period) OVER (PARTITION BY promotion_promo_code_2 ORDER BY ends_on_period ASC, id ASC, promo_id ASC, promo_id_2 ASC) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM matched
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY promotion_promo_code_2 ORDER BY ends_on_period ASC, id ASC, promo_id ASC, promo_id_2 ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(ends_on_period) OVER (PARTITION BY promotion_promo_code_2 ORDER BY ends_on_period ASC, id ASC, promo_id ASC, promo_id_2 ASC) IS NULL
                 OR ends_on_period > LAG(ends_on_period) OVER (PARTITION BY promotion_promo_code_2 ORDER BY ends_on_period ASC, id ASC, promo_id ASC, promo_id_2 ASC) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM islands
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY promotion_promo_code_2 ORDER BY ends_on_period ASC, id ASC, promo_id ASC, promo_id_2 ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', ends_on_period) AS quarter_start,
    MAX(ends_on_period) AS last_seen,
    MIN(ends_on_period) AS first_ends_on_period,
    COUNT(*) FILTER (WHERE channel = 'partner') AS partner_rows
FROM sessionized
GROUP BY quarter_start
HAVING partner_rows < 1000
ORDER BY partner_rows ASC;
