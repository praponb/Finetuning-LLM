WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY opened_date NULLS FIRST, id) AS rank_in_group
    FROM accounts
    QUALIFY rank_in_group = 1
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.age,
        p.city,
        p.country
    FROM latest src
    JOIN customers p
        ON src.customer_id <=> p.id
    WHERE p.country NOT IN ('SG', 'US')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY age DESC, id) AS latest_rank
        FROM joined
    ) ranked
    WHERE latest_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(age) OVER (PARTITION BY account_type, city) AS account_type_sum_age,
        LAG(age, 1) OVER (PARTITION BY city, account_type ORDER BY balance, id) AS prev_age
    FROM ranked
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_date,
        status,
        rank_in_group,
        age,
        city,
        country,
        latest_rank,
        account_type_sum_age,
        prev_age,
        SUM(balance) OVER w_running AS running_sum_balance,
        AVG(balance) OVER w_running AS running_avg_balance,
        MAX(balance) OVER w_running AS running_max_balance
    FROM enriched
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY prev_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(age) OVER (PARTITION BY city ORDER BY running_max_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_age,
        MIN(age) OVER (PARTITION BY city ORDER BY running_max_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_age,
        COUNT(age) OVER (PARTITION BY city ORDER BY running_max_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_age,
        SUM(age) OVER (PARTITION BY city ORDER BY running_max_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_age
    FROM scored
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.rank_in_group,
        s.latest_rank,
        s.account_type_sum_age,
        s.prev_age,
        s.running_max_balance,
        s.running_avg_age,
        s.running_min_age,
        s.running_sum_age,
        latest.transaction_amount
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Window metrics over several different partitions
SELECT
    id,
    rank_in_group,
    prev_age,
    running_max_balance,
    running_min_age,
    running_sum_age,
    transaction_amount,
    COUNT(running_sum_age) OVER (ORDER BY running_min_age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_running_sum_age,
    AVG(running_sum_age) OVER (ORDER BY running_min_age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_running_sum_age,
    MAX(running_sum_age) OVER (ORDER BY running_min_age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_running_sum_age,
    LEAD(running_avg_age, 2) OVER (ORDER BY running_min_age, id) AS next_running_avg_age
FROM with_latest
