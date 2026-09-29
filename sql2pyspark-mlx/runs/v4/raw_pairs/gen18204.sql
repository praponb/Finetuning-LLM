with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        tag
    from carts
    lateral view outer explode(coupon_codes) x as tag
),
enriched as (
    -- Attach customers attributes
    select
        e.*,
        p.segment,
        p.age
    from exploded e
    left join customers p
        on e.customer_id = p.id
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        sum(age) over (order by cast(created_at as bigint) range between 3600 preceding and current row) as age_sum_1h,
        dense_rank() over w_recent as age_dense_rank,
        avg(age) over w_ordered as customer_id_avg_age,
        round(age / sum(age) over (partition by segment), 4) as age_share,
        count(age) over (order by cast(created_at as bigint) range between 1800 preceding and current row) as age_count_30m
    from enriched
    window w_recent as (partition by customer_id, tag order by age desc nulls last), w_ordered as (partition by customer_id)
),
joined as (
    -- Attach customers attributes
    select e.*, p.city
    from metrics e
    left join customers p
        on e.customer_id = p.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        round(customer_id_avg_age / sum(customer_id_avg_age) over (partition by city), 4) as customer_id_avg_age_share,
        rank() over w_recent as customer_id_avg_age_rank,
        max(age_count_30m) over w_hist as customer_id_max_age_count_30m,
        max(age_count_30m) over w_running as age_count_30m_max_2h,
        percent_rank() over w_grp as age_share_percent_rank
    from joined
    window w_recent as (partition by customer_id, tag order by customer_id_avg_age nulls first), w_hist as (partition by customer_id), w_running as (order by cast(created_at as bigint) range between 7200 preceding and current row), w_grp as (partition by city, segment order by age_share nulls first)
),
remaining_keys as (
    -- Set intersection on keys
    select id, customer_id
    from with_windows
    intersect
    select id, customer_id
    from carts
    where hour(created_at) between 9 and 14
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join remaining_keys k on s.id = k.id and s.customer_id = k.customer_id
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            tag,
            customer_id,
            segment,
            customer_id_avg_age
        from kept_rows
    ) src
    pivot (
        sum(customer_id_avg_age) as amount
        for segment in ('SMB', 'Consumer')
    )
)
select
    tag,
    customer_id,
    SMB,
    Consumer,
    case when tag rlike '^(VIP|WELCOME5)$' then 'GROUP_A' when tag rlike '^(SPRING10|BUNDLE)$' then 'PRIORITY' else 'OTHER_ZONE' end as tag_zone
from by_type
