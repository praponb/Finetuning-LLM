with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        method,
        paid_at,
        status,
        date_format(paid_at, 'yyyy-MM') as paid_at_label
    from payments
    where method != 'bank_transfer'
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval '30' day then 1 else 0 end as is_new_session
    from base
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from sessionized
    qualify row_number() over (partition by status order by burst_id, id) = 1
)
select
    id,
    method,
    paid_at,
    status,
    paid_at_label,
    burst_id,
    case when status rlike '^(SUCCESS|FAILED)$' then 'EXTENDED' when status rlike '^(REFUNDED)$' then 'CORE' else 'GROUP_B' end as status_zone
from latest;
