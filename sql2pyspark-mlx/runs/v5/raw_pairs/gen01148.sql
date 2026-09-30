WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        start_date,
        end_date,
        status,
        INITCAP(TRIM(plan)) AS plan_title
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND plan = 'basic'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        plan_title,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC, id) AS seq_no_2
    FROM valid
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN seq_no_2 >= 8 THEN 'High' WHEN seq_no_2 >= 4 THEN 'Medium' ELSE 'Low' END AS seq_no_2_band
    FROM metrics
    WHERE seq_no IS NOT NULL
),
base_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(seq_no * 1.07, 2) AS seq_no_adjusted
    FROM base
    WHERE seq_no <> 6
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        seq_no,
        seq_no_adjusted,
        SUM(seq_no_adjusted) OVER (ORDER BY seq_no_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no_adjusted,
        AVG(seq_no) OVER w_hist AS seq_no_avg_6_prior
    FROM base_subscriptions
    WHERE seq_no > 6
    WINDOW w_hist AS (ORDER BY seq_no, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
)
SELECT
    id,
    seq_no_adjusted,
    running_seq_no_adjusted,
    seq_no_avg_6_prior
FROM scored
ORDER BY running_seq_no_adjusted DESC NULLS LAST
