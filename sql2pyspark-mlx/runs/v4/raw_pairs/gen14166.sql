with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        concat_ws('-', status, account_type) as status_account_type_key,
        last_day(opened_date) as opened_date_month_end
    from accounts
    where customer_id is not null
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(status, true) over (order by balance, id rows between unbounded preceding and current row) as first_known_status,
        row_number() over (partition by account_type, status order by balance desc, id) as event_seq
    from filtered
),
enriched as (
    -- Attach customers attributes
    select e.*, b.name
    from metrics e
    left join customers b
        on e.customer_id = b.id
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        initcap(trim(account_type)) as account_type_title,
        upper(account_type) as account_type_upper
    from enriched
    where customer_id is not null
),
enriched_accounts as (
    -- Ranking and gap features
    select
        *,
        lag(balance, 2, 0) over (order by event_seq, id) as prev_balance,
        sum(balance) over w_grp as running_balance,
        round(event_seq / sum(event_seq) over (partition by customer_id), 4) as event_seq_share
    from valid
    window w_grp as (partition by account_type order by opened_date_month_end, id rows between unbounded preceding and current row)
),
latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by customer_id order by opened_date_month_end desc nulls first, id) as rank_in_group
    from enriched_accounts
    qualify rank_in_group = 1
)
-- Native SQL PIVOT
select *
from (
    select
        customer_id,
        account_type,
        prev_balance
    from latest
) src
pivot (
    sum(prev_balance) as amount,
    count(1) as cnt
    for account_type in ('LOAN', 'SAVINGS')
);
