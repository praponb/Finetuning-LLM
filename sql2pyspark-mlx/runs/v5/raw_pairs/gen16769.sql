with scored as (
    -- Window metrics over several different partitions
    select
        id,
        txn_type,
        merchant,
        is_flagged,
        lag(amount) over w_part as prev_amount,
        row_number() over w_part as row_seq,
        last_value(merchant) ignore nulls over (order by txn_time, id rows between unbounded preceding and current row) as last_known_merchant
    from transactions
    window w_part as (partition by merchant, txn_type order by amount, id)
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        avg(prev_amount) over (partition by merchant, txn_type order by row_seq, id rows between 6 preceding and current row) as prev_amount_avg_6,
        round(prev_amount / sum(prev_amount) over (partition by merchant), 4) as prev_amount_share,
        rank() over (partition by txn_type order by prev_amount desc) as prev_amount_rank
    from scored
),
scored_transactions as (
    -- Window metrics over several different partitions
    select
        id,
        merchant,
        prev_amount,
        prev_amount_share,
        prev_amount_rank,
        lag(prev_amount_rank) over (partition by merchant order by row_seq, id) as prev_prev_amount_rank,
        lead(prev_amount_share) over (partition by merchant order by row_seq, id) as next_prev_amount_share,
        lag(prev_amount_share) over (partition by merchant order by row_seq, id) as prev_prev_amount_share,
        row_number() over (partition by merchant order by row_seq, id) as row_seq_2,
        ntile(10) over (partition by merchant order by row_seq desc, id) as row_seq_bucket
    from with_windows
    where is_flagged = false
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from scored_transactions
    qualify row_number() over (partition by merchant order by prev_prev_amount_share desc, id) = 1
),
kept_keys as (
    -- Set difference on keys
    select id, merchant
    from latest
    except
    select id, merchant
    from transactions
    where to_date(txn_time) < '2024-01-25'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    inner join kept_keys k on s.id = k.id and s.merchant = k.merchant
),
with_windows_transactions as (
    -- Running and moving aggregates
    select
        id,
        prev_amount,
        prev_amount_share,
        prev_prev_amount_share,
        row_seq_bucket,
        dense_rank() over (partition by merchant order by prev_prev_amount_share desc nulls last) as prev_prev_amount_share_dense_rank,
        round(prev_prev_amount_rank / sum(prev_prev_amount_rank) over (partition by merchant), 4) as prev_prev_amount_rank_share
    from reconciled
)
-- Grouped metrics
select
    case when prev_prev_amount_share >= 0.64 then 'HIGH' else 'LOW' end as prev_prev_amount_share_tier,
    case when prev_prev_amount_rank_share >= 0.65 then 'PREMIUM' else 'STANDARD' end as prev_prev_amount_rank_share_tier,
    count(*) as event_count,
    max(prev_prev_amount_share_dense_rank) as max_prev_prev_amount_share_dense_rank,
    count(*) as row_count
from with_windows_transactions
group by prev_prev_amount_share_tier, prev_prev_amount_rank_share_tier
having row_count >= 1
order by prev_prev_amount_rank_share_tier desc nulls last;
