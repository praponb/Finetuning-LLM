WITH joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.country,
        d.segment,
        d.credit_limit
    FROM subscriptions src
    INNER JOIN customers d
        ON src.customer_id = d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        monthly_fee,
        country,
        segment,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY end_date DESC, id) AS event_seq,
        FIRST_VALUE(country, TRUE) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
    FROM joined
    WHERE customer_id IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        event_seq * monthly_fee AS event_seq_times_monthly_fee,
        LEAST(monthly_fee, event_seq) AS least_monthly_fee_event_seq,
        COALESCE(country, 'UNKNOWN') AS country_clean
    FROM with_windows
    WHERE segment IS NOT NULL
      AND customer_id IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY monthly_fee DESC NULLS LAST, id) AS rank_in_group
        FROM valid
    ) ranked
    WHERE rank_in_group = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(event_seq AS DOUBLE) AS event_seq_str,
        COALESCE(country, 'N/A') AS country_filled
    FROM deduped
    WHERE country = 'US'
)
SELECT
    s.id,
    s.customer_id,
    s.segment,
    s.event_seq,
    s.first_known_country,
    s.event_seq_times_monthly_fee,
    s.least_monthly_fee_event_seq,
    s.country_clean,
    s.rank_in_group,
    (SELECT AVG(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM filtered s
ORDER BY segment
