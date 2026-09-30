with valid as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        upper(status) as status_upper,
        coalesce(carrier, 'DIRECT') as resolved_carrier
    from shipments
    where carrier is not null
      and cost > 65
),
with_dims as (
    -- Attach orders attributes
    select
        a.*,
        d.status as order_status,
        d.order_date
    from valid a
    left join orders d
        on a.order_id = d.id
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        shipped_date,
        delivered_date,
        status_upper,
        resolved_carrier,
        order_status,
        order_date,
        case when status in ('DELIVERED', 'IN_TRANSIT') then 'Group A' else 'Other' end as status_group
    from with_dims
    where shipped_date > to_date('2023-06-16')
      and carrier is not null
      and year(shipped_date) >= 2024
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by order_id order by shipped_date, id) as rank_in_group
        from filtered
    ) x
    where rank_in_group = 1
),
totals as (
    -- Aggregate with conditional metrics
    select
        date_trunc('quarter', delivered_date) as quarter_start,
        order_status,
        count(*) as event_count,
        count(*) as cnt,
        sort_array(collect_set(order_status)) as order_status_list
    from ranked
    group by date_trunc('quarter', delivered_date), order_status
)
select
    quarter_start,
    order_status,
    event_count,
    order_status_list,
    case when order_status rlike '^(COMPLETED|SHIPPED)$' then 'PRIORITY' else 'OTHER_ZONE' end as order_status_zone
from totals;
