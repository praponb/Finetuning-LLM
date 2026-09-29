with eligible as (
    select s.*
    from accounts s
    where not exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.txn_time < s.opened_date
        )
),
deduped as (
    select
        id,
        opened_date,
        status
    from eligible
    qualify row_number() over (partition by account_type order by opened_date nulls last, id) = 1
),
valid as (
    select
        *,
        date_trunc('year', opened_date) as opened_date_period,
        coalesce(status, 'DIRECT') as status_filled
    from deduped
    where opened_date > date '2023-08-23'
      and status is not null
),
kept as (
    select s.*
    from valid s
    where exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.txn_time >= s.opened_date_period
        )
)
select
    status,
    (unix_timestamp(max(opened_date_period)) - unix_timestamp(min(opened_date_period))) / 60 as span_minutes,
    (unix_timestamp(max(opened_date_period)) - unix_timestamp(min(opened_date_period))) / 60 as duration_min,
    (unix_timestamp(max(opened_date_period)) - unix_timestamp(min(opened_date_period))) / 60 as duration_min_2
from kept
group by status
order by duration_min_2 desc nulls first
