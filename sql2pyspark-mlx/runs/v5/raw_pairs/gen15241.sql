with flattened as (
    select
        id,
        created_at,
        coupon_codes,
        key,
        value
    from carts
    lateral view explode(attributes) e as key, value
),
with_windows as (
    select
        *,
        last_value(key, true) over (partition by key order by created_at, id, key rows between unbounded preceding and current row) as last_known_key,
        row_number() over (partition by key order by created_at desc, id, key) as row_seq,
        row_number() over (partition by key order by created_at desc, id, key) as event_seq,
        first_value(key) ignore nulls over (partition by key order by created_at, id, key rows between unbounded preceding and current row) as first_known_key,
        first_value(key) ignore nulls over (partition by key order by created_at, id, key rows between unbounded preceding and current row) as first_known_key_2
    from flattened
),
deduped as (
    select *
    from with_windows
    qualify rank() over (partition by key order by event_seq nulls first) = 1
),
deduped_carts as (
    select
        id,
        coupon_codes,
        key,
        row_seq
    from (
        select *, row_number() over (partition by key order by created_at desc, id, key) as latest_rank
        from deduped
    ) ranked
    where latest_rank = 1
),
latest as (
    select *
    from (
        select *, row_number() over (partition by key order by row_seq nulls last, id, key) as row_num
        from deduped_carts
    ) x
    where row_num = 1
),
stats as (
    select
        key,
        count_if(key = 'device') as device_events,
        round(sum(row_seq) / nullif(count(case when key = 'utm_source' then 1 end), 0), 2) as row_seq_per_utm_source,
        round(sum(row_seq) / nullif(count(case when key = 'gift' then 1 end), 0), 2) as row_seq_per_gift,
        count(distinct key) as distinct_keys
    from latest
    group by key
)
select
    key,
    device_events,
    row_seq_per_gift,
    distinct_keys,
    case when key rlike '^(device|gift)$' then 'CORE' when key rlike '^(utm_source|weight_kg)$' then 'OTHER_ZONE' else 'GROUP_B' end as key_zone
from stats
order by device_events desc
