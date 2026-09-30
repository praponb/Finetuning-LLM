with base as (
    -- Basic cleansing and defaults
    select
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        length(merchant) as merchant_length,
        coalesce(merchant, 'UNKNOWN') as merchant_clean
    from transactions
    where merchant is not null
),
joined as (
    -- Attach accounts attributes
    select
        s.*,
        a.status,
        a.account_type,
        a.balance
    from base s
    left join accounts a
        on s.account_id = a.id
),
metrics as (
    -- Ranking and gap features
    select
        id,
        account_id,
        txn_time,
        account_type,
        balance,
        count(amount) over w_ordered as last7_count_amount,
        max(amount) over w_ordered as last7_max_amount,
        sum(amount) over w_ordered as last7_sum_amount,
        row_number() over w_running as seq_no
    from joined
    window w_ordered as (partition by account_type, merchant order by balance, id rows between 6 preceding and current row), w_running as (partition by account_type order by balance, id)
),
enriched as (
    -- Attach accounts attributes
    select
        s.id,
        s.account_id,
        s.txn_time,
        s.balance,
        s.last7_count_amount,
        s.last7_sum_amount,
        s.seq_no,
        d.opened_date,
        d.account_type as account_account_type,
        d.status
    from metrics s
    left join accounts d
        on s.account_id = d.id
    where d.account_type in ('LOAN', 'CHECKING', 'CREDIT')
),
metrics_transactions as (
    -- Ranking and gap features
    select
        id,
        balance,
        last7_count_amount,
        opened_date,
        status,
        lead(seq_no) over (partition by account_account_type order by seq_no, id) as next_seq_no,
        lead(last7_count_amount) over (partition by account_account_type order by seq_no, id) as next_last7_count_amount,
        row_number() over (partition by account_account_type order by seq_no, id) as seq_no_2
    from enriched
)
-- Grouped metrics
select
    case when seq_no_2 >= 7 then 'LARGE' else 'SMALL' end as seq_no_2_tier,
    count(case when status = 'CLOSED' then 1 end) as closed_count,
    sum(case when status = 'CLOSED' then balance else 0 end) as closed_balance,
    count(*) filter (where status = 'CLOSED') as closed_rows,
    approx_percentile(next_last7_count_amount, 0.95) as next_last7_count_amount_p95,
    round(sum(seq_no_2) / nullif(count(case when status = 'FROZEN' then 1 end), 0), 2) as seq_no_2_per_frozen,
    grouping(case when seq_no_2 >= 7 then 'LARGE' else 'SMALL' end) as grouping_id
from metrics_transactions
group by rollup (case when seq_no_2 >= 7 then 'LARGE' else 'SMALL' end);
