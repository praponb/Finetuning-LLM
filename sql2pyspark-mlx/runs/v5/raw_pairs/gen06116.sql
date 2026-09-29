with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        concat(status, ' ', account_type) as status_account_type,
        coalesce(status, 'DIRECT') as status_clean
    from accounts
    where opened_date >= date '2023-09-13'
      and opened_date <= '2025-07-30'
),
metrics as (
    -- Ranking and gap features
    select
        *,
        max(balance) over (partition by status) as status_max_balance,
        dense_rank() over (partition by customer_id, account_type order by balance desc nulls first) as balance_dense_rank,
        row_number() over (order by balance, id) as row_seq
    from base
),
enriched as (
    -- Attach customers attributes
    select src.*, d.name
    from metrics src
    left join customers d
        on src.customer_id = d.id
),
matched as (
    -- Non-equi join on a validity window
    select
        a.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from enriched a
    left join subscriptions sub
        on a.customer_id = sub.customer_id
       and a.opened_date between sub.start_date and sub.end_date
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        account_type,
        opened_date,
        status,
        status_account_type,
        status_clean,
        status_max_balance,
        balance_dense_rank,
        row_seq,
        name,
        subscription_id,
        plan,
        monthly_fee,
        round(balance_dense_rank * 0.9, 2) as balance_dense_rank_scaled,
        coalesce(row_seq, 0) as row_seq_filled
    from matched
    where opened_date >= date '2023-05-04'
      and subscription_id = 4
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        opened_date,
        status_clean,
        status_max_balance,
        row_seq,
        name,
        subscription_id,
        row_number() over (partition by account_type order by row_seq_filled desc, id, subscription_id) as seq_no,
        ntile(3) over (partition by account_type order by subscription_id, id, subscription_id) as subscription_id_bucket
    from cleaned
    where row_seq_filled > 64
)
-- Aggregate with conditional metrics
select
    case when status_max_balance >= 72915 then 'PREMIUM' else 'STANDARD' end as status_max_balance_tier,
    count(distinct id) as distinct_ids,
    approx_percentile(subscription_id, 0.95) as subscription_id_p95,
    approx_percentile(row_seq, 0.5) as row_seq_p50
from with_windows
group by status_max_balance_tier
having approx_percentile(subscription_id, 0.95) > 0
order by distinct_ids;
