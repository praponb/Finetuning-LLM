with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        k,
        value
    from carts
    lateral view outer explode(attributes) e as k, value
),
enriched as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.k,
        src.value,
        d.segment,
        d.credit_limit,
        d.email
    from flattened src
    join customers d
        on src.customer_id <=> d.id
    where d.country in ('DE', 'US', 'TH')
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        value,
        segment,
        email,
        upper(k) as k_upper,
        coalesce(segment, 'unassigned') as segment_filled,
        coalesce(k, 'NONE') as k_clean
    from enriched
    where customer_id is not null
      and segment is not distinct from 'SMB'
),
enriched_carts as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.value,
        a.segment,
        a.k_upper,
        a.segment_filled,
        b.segment as customer_segment
    from filtered a
    left join customers b
        on a.customer_id = b.id
)
-- Aggregate with conditional metrics
select
    customer_segment,
    count(case when customer_segment = 'Corporate' then 1 end) as corporate_count,
    size(collect_set(customer_segment)) as num_customer_segments,
    count(*) as row_count,
    sort_array(collect_set(segment)) as segment_list
from enriched_carts
group by customer_segment
having row_count > 1
order by row_count
