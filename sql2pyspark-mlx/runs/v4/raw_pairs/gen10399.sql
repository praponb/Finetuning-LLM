with base as (
    -- Keep recent, valid rows
    select
        id,
        balance,
        opened_date,
        status,
        trim(account_type) clean_account_type,
        coalesce(status, 'N/A') status_clean
    from accounts
    where opened_date > date '2024-02-20'
      and customer_id is not null
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by status order by opened_date desc, id) as rn
    from base
    qualify rn = 1
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        balance,
        opened_date,
        status,
        clean_account_type,
        status_clean,
        rn,
        avg(balance) over w_recent balance_avg_5_prior,
        first_value(status, true) over w_ordered first_known_status
    from ranked
    window w_recent as (order by opened_date, id rows between 5 preceding and 1 preceding), w_ordered as (partition by status order by balance, id rows between unbounded preceding and current row)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by status order by opened_date, id) as rank_in_group
        from metrics
    ) x
    where rank_in_group = 1
)
select
    id,
    clean_account_type,
    status_clean,
    balance_avg_5_prior,
    first_known_status,
    rank_in_group
from latest
