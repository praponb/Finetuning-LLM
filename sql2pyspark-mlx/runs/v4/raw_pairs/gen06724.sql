with enriched as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        ntile(4) over (order by balance, id) as balance_bucket,
        dense_rank() over w_hist as balance_dense_rank
    from accounts
    window w_hist as (partition by account_type, customer_id order by balance)
),
matched_keys as (
    -- Set intersection on keys
    select id, account_type
    from enriched
    intersect all
    select id, account_type
    from accounts
    where opened_date < '2024-08-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join matched_keys k on s.id = k.id and s.account_type = k.account_type
),
enriched_accounts as (
    -- Ranking and gap features
    select
        *,
        count(balance) over (partition by account_type) as account_type_count_balance,
        cume_dist() over (partition by customer_id order by balance desc nulls first) as balance_cume_dist,
        sum(balance) over (partition by account_type) as account_type_sum_balance,
        round(balance_dense_rank / sum(balance_dense_rank) over (partition by account_type), 4) as balance_dense_rank_share
    from verified
)
select
    id,
    opened_date,
    balance_dense_rank,
    account_type_count_balance,
    balance_cume_dist,
    account_type_sum_balance,
    balance_dense_rank_share
from enriched_accounts
order by balance_cume_dist desc, opened_date desc
