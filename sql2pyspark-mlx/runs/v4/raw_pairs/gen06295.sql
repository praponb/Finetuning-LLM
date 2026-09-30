WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label,
        YEAR(end_date) AS end_date_year
    FROM subscriptions
    WHERE end_date > TO_DATE('2023-08-14')
      AND customer_id IS NOT NULL
      AND end_date < '2025-08-03'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY end_date_year DESC NULLS LAST, id) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id) AS latest_rank
    FROM deduped
    QUALIFY latest_rank = 1
),
base_subscriptions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        start_date_label,
        end_date_year,
        rank_in_group,
        latest_rank,
        LEAST(end_date_year, rank_in_group) AS least_end_date_year_rank_in_group
    FROM latest
    WHERE customer_id IS NOT NULL
      AND latest_rank >= 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY status, customer_id ORDER BY end_date_year, id) AS end_date_year_bucket,
        AVG(end_date_year) OVER (PARTITION BY plan ORDER BY latest_rank, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS end_date_year_avg_5_prior,
        NTILE(10) OVER w_part AS end_date_year_bucket_2,
        SUM(end_date_year) OVER w_ordered AS status_sum_end_date_year
    FROM base_subscriptions
    WINDOW w_part AS (ORDER BY end_date_year DESC, id), w_ordered AS (PARTITION BY status, plan)
),
matched_keys AS (
    -- Set difference on keys
    SELECT plan, status
    FROM enriched
    EXCEPT ALL
    SELECT plan, status
    FROM subscriptions
    WHERE plan NOT IN ('enterprise', 'standard')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.plan = k.plan AND s.status = k.status
)
SELECT
    s.id,
    s.customer_id,
    s.plan,
    s.monthly_fee,
    s.status,
    s.start_date_label,
    s.end_date_year,
    s.rank_in_group,
    s.latest_rank,
    (SELECT MIN(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS min_account_balance
FROM reconciled s
ORDER BY latest_rank DESC;
