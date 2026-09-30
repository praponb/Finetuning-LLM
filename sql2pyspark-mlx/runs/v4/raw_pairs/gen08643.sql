WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        TO_DATE(started_at) AS started_at_date
    FROM sessions
    WHERE started_at > TO_DATE('2023-12-31')
      AND browser IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY device ORDER BY session_no DESC) AS session_no_percent_rank,
        SUM(session_no) OVER (PARTITION BY device ORDER BY CAST(started_at AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS session_no_sum_90d,
        SUM(new_group_flag) OVER (PARTITION BY device ORDER BY started_at_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_new_group_flag,
        DENSE_RANK() OVER (PARTITION BY browser ORDER BY new_group_flag DESC NULLS LAST) AS new_group_flag_dense_rank,
        SUM(session_no) OVER (PARTITION BY country ORDER BY new_group_flag ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_session_no
    FROM islands
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY RANK() OVER (PARTITION BY device, customer_id ORDER BY started_at_date DESC NULLS FIRST) = 1
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.email,
        d.country AS customer_country
    FROM ranked a
    LEFT JOIN customers d
        ON a.customer_id = d.id
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    country,
    started_at_date,
    new_group_flag,
    session_no
FROM enriched
ORDER BY started_at_date ASC
