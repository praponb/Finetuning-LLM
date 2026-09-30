with cleaned as (
    select
        *,
        coalesce(sqft, 0) as sqft_filled,
        upper(store_name) as store_name_upper
    from stores
    where opened_date >= to_date('2023-03-30')
),
deduped as (
    select
        id,
        city,
        region,
        sqft
    from (
        select *, row_number() over (partition by city order by sqft desc nulls first, id) as rank_in_group
        from cleaned
    ) x
    where rank_in_group = 1
),
latest as (
    select *, row_number() over (partition by city order by sqft nulls last, id) as row_num
    from deduped
    qualify row_num = 1
),
latest_match as (
    select
        s.id,
        s.sqft,
        top1.order_status,
        top1.order_order_date
    from latest s
    join lateral (
            select c.status as order_status, c.order_date as order_order_date
            from orders c
            where c.store_id = s.id
            order by c.discount desc, c.id desc
            limit 1
        ) top1 on true
),
ranked as (
    select
        id,
        sqft,
        order_status
    from (
        select *, row_number() over (partition by order_status order by sqft, id) as rank_in_group
        from latest_match
    ) x
    where rank_in_group = 1
),
with_windows as (
    select
        *,
        avg(sqft) over w_part as running_avg_sqft,
        max(sqft) over w_part as running_max_sqft,
        min(sqft) over w_part as running_min_sqft,
        sum(sqft) over w_part as running_sum_sqft,
        sum(sqft) over (order by sqft, id rows between unbounded preceding and current row) as running_sqft
    from ranked
    window w_part as (partition by order_status order by sqft, id rows between unbounded preceding and current row)
)
select
    id,
    sqft,
    order_status,
    running_max_sqft,
    running_min_sqft,
    case when order_status rlike '^(CANCELLED|COMPLETED)$' then 'GROUP_B' when order_status rlike '^(PENDING|SHIPPED)$' then 'OTHER_ZONE' else 'PRIORITY' end as order_status_zone
from with_windows
order by running_max_sqft desc, order_status;
