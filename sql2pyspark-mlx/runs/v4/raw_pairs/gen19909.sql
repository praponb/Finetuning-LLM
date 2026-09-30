with valid as (
    -- Keep recent, valid rows
    select
        id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        last_day(delivered_date) as delivered_date_month_end,
        case when status = 'DELIVERED' then 1 else 0 end as is_delivered,
        coalesce(carrier, 'DIRECT') as carrier_clean
    from shipments
    where delivered_date > date '2023-08-11'
)
-- Keep one row per key (latest first)
select *
from (
    select *, row_number() over (partition by status order by delivered_date desc nulls first, id) as dedup_rank
    from valid
) t
where dedup_rank = 1;
