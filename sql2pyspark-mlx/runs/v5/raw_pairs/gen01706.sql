with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        attr_key,
        value
    from carts
    lateral view explode(attributes) x as attr_key, value
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by attr_key order by created_at, id, attr_key) as dedup_rank
        from exploded
    ) x
    where dedup_rank = 1
),
enriched as (
    -- Ranking and gap features
    select
        id,
        attr_key,
        row_number() over (order by created_at desc, id, attr_key) as row_seq,
        row_number() over w_grp as row_seq_2,
        row_number() over w_grp as seq_no
    from deduped
    window w_grp as (partition by attr_key order by created_at desc, id, attr_key)
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        attr_key,
        row_seq,
        row_seq_2,
        row_seq + seq_no as row_seq_plus_seq_no
    from enriched
    where attr_key is not null
      and attr_key is not distinct from 'weight_kg'
)
-- Aggregate with conditional metrics
select
    case when row_seq_plus_seq_no >= 69 then 'PREMIUM' else 'STANDARD' end as row_seq_plus_seq_no_tier,
    case when row_seq >= 7 then 'PREMIUM' else 'STANDARD' end as row_seq_tier,
    sort_array(collect_set(attr_key)) as attr_key_list,
    count(*) filter (where attr_key = 'device') as device_rows,
    sum(row_seq_plus_seq_no) as total_row_seq_plus_seq_no
from cleaned
group by cube (case when row_seq_plus_seq_no >= 69 then 'PREMIUM' else 'STANDARD' end, case when row_seq >= 7 then 'PREMIUM' else 'STANDARD' end)
order by row_seq_tier
