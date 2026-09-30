WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        amount,
        status,
        CASE WHEN method IN ('paypal', 'cash') THEN 'Priority' ELSE 'Other' END method_group,
        COALESCE(method, 'unassigned') method_filled
    FROM payments
    WHERE paid_at > DATE '2023-10-27'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount) OVER w_hist running_avg_amount,
        COUNT(amount) OVER w_hist running_count_amount,
        SUM(amount) OVER w_hist running_sum_amount
    FROM valid
    WHERE amount BETWEEN 1030 AND 1330
    WINDOW w_hist AS (PARTITION BY status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        amount,
        status,
        method_filled,
        running_avg_amount,
        running_sum_amount,
        running_count_amount * amount running_count_amount_times_amount,
        running_sum_amount + running_count_amount running_sum_amount_plus_running_count_amount
    FROM with_windows
    WHERE order_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status,
        method_filled,
        running_avg_amount,
        running_sum_amount,
        AVG(running_sum_amount_plus_running_count_amount) OVER (PARTITION BY status ORDER BY running_sum_amount_plus_running_count_amount, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) running_sum_amount_plus_running_count_amount_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY running_avg_amount, id) seq_no
    FROM filtered
),
with_windows_payments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status,
        running_avg_amount,
        running_sum_amount_plus_running_count_amount_avg_5_prior,
        seq_no,
        SUM(seq_no) OVER (PARTITION BY status ORDER BY running_avg_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_seq_no,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY running_avg_amount DESC, id) event_seq,
        SUM(seq_no) OVER (PARTITION BY status ORDER BY running_sum_amount_plus_running_count_amount_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_seq_no
    FROM metrics
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY event_seq NULLS LAST, id) AS latest_rank
    FROM with_windows_payments
    QUALIFY latest_rank = 1
)
SELECT
    id,
    status,
    running_avg_amount,
    running_sum_amount_plus_running_count_amount_avg_5_prior,
    cumulative_seq_no,
    event_seq,
    running_seq_no
FROM deduped
