with base as (
    -- Keep recent, valid rows
    select *, upper(account_type) as account_type_upper
    from accounts
    where opened_date >= date '2023-04-05'
      and status is distinct from 'OPEN'
),
kept_keys as (
    -- Set difference on keys
    select customer_id
    from base
    except
    select customer_id
    from accounts
    where opened_date > '2025-02-14'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join kept_keys k on s.customer_id = k.customer_id
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        concat(status, '/', account_type) as status_account_type,
        coalesce(status, 'DIRECT') as status_clean
    from kept_rows
    where opened_date > date '2023-09-10'
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        account_type_upper,
        status_account_type,
        status_clean,
        cast(balance as int) as balance_int,
        substring(account_type, 1, 4) as account_type_prefix
    from filtered
    where customer_id is not null
),
base_accounts as (
    -- Basic cleansing and defaults
    select
        customer_id,
        opened_date,
        status,
        account_type_upper,
        status_account_type,
        status_clean,
        balance_int,
        account_type_prefix,
        date_format(opened_date, 'yyyy-MM-dd') as opened_date_label,
        coalesce(status, 'N/A') as resolved_status
    from valid
    where year(opened_date) = 2024
),
stats as (
    -- Multi-level aggregation
    select
        customer_id,
        avg(balance_int) filter (where status = 'OPEN') as open_avg_balance_int,
        count_if(status = 'FROZEN') as frozen_events,
        max(balance_int) filter (where status = 'OPEN') as open_max_balance_int,
        percentile_approx(balance_int, 0.95) as balance_int_p95,
        count(*) filter (where status = 'OPEN') as open_rows,
        approx_percentile(balance_int, array(0.25, 0.5, 0.75)) as balance_int_quartiles
    from base_accounts
    group by customer_id
)
select
    customer_id,
    open_avg_balance_int,
    frozen_events,
    balance_int_p95,
    open_rows,
    balance_int_quartiles
from stats
order by open_rows desc nulls first
