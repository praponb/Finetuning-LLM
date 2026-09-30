WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DAYOFWEEK(delivered_date) AS delivered_date_dayofweek
    FROM shipments
    WHERE carrier IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        carrier,
        delivered_date,
        status,
        delivered_date_dayofweek
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS LAST, id) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(delivered_date_dayofweek) OVER w_grp AS running_count_delivered_date_dayofweek,
        MIN(delivered_date_dayofweek) OVER w_grp AS running_min_delivered_date_dayofweek,
        AVG(delivered_date_dayofweek) OVER w_grp AS running_avg_delivered_date_dayofweek
    FROM deduped
    WINDOW w_grp AS (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_shipments AS (
    -- Running and moving aggregates
    SELECT
        id,
        carrier,
        delivered_date,
        delivered_date_dayofweek,
        running_min_delivered_date_dayofweek,
        running_avg_delivered_date_dayofweek,
        NTILE(5) OVER (ORDER BY delivered_date_dayofweek, id) AS delivered_date_dayofweek_bucket,
        ROW_NUMBER() OVER w_grp AS row_seq,
        SUM(running_count_delivered_date_dayofweek) OVER (ORDER BY running_count_delivered_date_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_count_delivered_date_dayofweek
    FROM metrics
    WINDOW w_grp AS (PARTITION BY carrier, status ORDER BY delivered_date_dayofweek, id)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(running_avg_delivered_date_dayofweek) OVER (ORDER BY running_running_count_delivered_date_dayofweek, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_avg_delivered_date_dayofweek_avg_5_prior,
        SUM(delivered_date_dayofweek) OVER (PARTITION BY carrier ORDER BY running_avg_delivered_date_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_delivered_date_dayofweek,
        SUM(running_avg_delivered_date_dayofweek) OVER (PARTITION BY carrier ORDER BY running_avg_delivered_date_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_avg_delivered_date_dayofweek,
        AVG(row_seq) OVER (PARTITION BY carrier ORDER BY delivered_date_dayofweek_bucket, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS row_seq_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY running_avg_delivered_date_dayofweek DESC, id) AS seq_no
    FROM metrics_shipments
),
deduped_shipments AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY carrier ORDER BY delivered_date DESC NULLS LAST) AS rank_in_group
        FROM with_windows
    ) ranked
    WHERE rank_in_group = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(running_avg_delivered_date_dayofweek / seq_no, 2) AS running_avg_delivered_date_dayofweek_per_seq_no,
        COALESCE(carrier, 'N/A') AS carrier_filled
    FROM deduped_shipments
    WHERE carrier IS NOT NULL
      AND running_min_delivered_date_dayofweek < 66
)
-- Multi-level aggregation
SELECT
    carrier,
    COUNT(*) AS cnt,
    MAX(running_avg_delivered_date_dayofweek_per_seq_no) FILTER (WHERE carrier = 'DHL') AS dhl_max_running_avg_delivered_date_dayofweek_per_seq_no
FROM cleaned
GROUP BY carrier
ORDER BY cnt NULLS LAST;
