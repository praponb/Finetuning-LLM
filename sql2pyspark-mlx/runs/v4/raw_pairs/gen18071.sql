with filtered as (
    -- Keep recent, valid rows
    select
        *,
        last_day(opened_date) as opened_date_month_end,
        coalesce(status, 'DIRECT') as status_filled
    from accounts
    where customer_id is not null
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by customer_id, status order by opened_date_month_end desc nulls first, id) as latest_rank
        from filtered
    ) t
    where latest_rank = 1
),
enriched as (
    -- Ranking and gap features
    select
        *,
        lead(balance, 2) over w_hist as next_balance,
        last_value(balance, true) over w_recent as last_known_balance,
        percent_rank() over (partition by account_type order by balance nulls last) as balance_percent_rank
    from deduped
    window w_hist as (partition by customer_id order by opened_date_month_end, id), w_recent as (partition by account_type order by balance, id rows between unbounded preceding and current row)
)
-- Running and moving aggregates
select
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    opened_date_month_end,
    status_filled,
    latest_rank,
    next_balance,
    last_known_balance,
    balance_percent_rank,
    lead(balance, 1) over (order by last_known_balance, id) as next_balance_2,
    rank() over (partition by status order by balance_percent_rank nulls last) as balance_percent_rank_rank
from enriched;
