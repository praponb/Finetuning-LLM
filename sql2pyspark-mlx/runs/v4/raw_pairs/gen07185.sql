WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        UPPER(plan) plan_upper
    FROM subscriptions
    WHERE end_date >= DATE '2024-04-12'
      AND customer_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY end_date DESC, id) event_seq,
        ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY end_date DESC, id) event_seq_2,
        ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY start_date DESC, id) seq_no
    FROM cleaned
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC NULLS LAST, id) AS dedup_rank
    FROM metrics
    QUALIFY dedup_rank = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM latest
    INTERSECT ALL
    SELECT id
    FROM subscriptions
    WHERE customer_id IN (18, 27)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN matched_keys k ON s.id = k.id
),
base AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(plan) plan_lower
    FROM verified
    WHERE customer_id IS NOT NULL
      AND status = 'CANCELLED'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        plan_upper,
        event_seq,
        event_seq_2,
        seq_no,
        dedup_rank,
        plan_lower,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY seq_no DESC) seq_no_percent_rank,
        CUME_DIST() OVER w_grp event_seq_2_cume_dist,
        PERCENT_RANK() OVER w_part event_seq_percent_rank
    FROM base
    WINDOW w_grp AS (PARTITION BY customer_id, status ORDER BY event_seq_2), w_part AS (ORDER BY event_seq DESC NULLS LAST)
),
matched_keys_2 AS (
    -- Set difference on keys
    SELECT plan
    FROM enriched
    EXCEPT
    SELECT plan
    FROM subscriptions
    WHERE start_date >= '2023-03-01'
),
verified_subscriptions AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys_2 k ON s.plan = k.plan
)
-- Window metrics over several different partitions
SELECT
    *,
    PERCENT_RANK() OVER w_recent seq_no_percent_rank_2,
    SUM(event_seq_2_cume_dist) OVER w_part customer_id_sum_event_seq_2_cume_dist,
    CUME_DIST() OVER (ORDER BY event_seq_percent_rank DESC NULLS LAST) event_seq_percent_rank_cume_dist,
    ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY plan), 4) seq_no_share,
    ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY status), 4) seq_no_share_2
FROM verified_subscriptions
WINDOW w_recent AS (PARTITION BY plan ORDER BY seq_no), w_part AS (PARTITION BY customer_id)
