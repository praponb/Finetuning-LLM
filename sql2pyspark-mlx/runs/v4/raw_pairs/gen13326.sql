with valid as (
    -- Basic cleansing and defaults
    select *, trim(status) as clean_status
    from orders
    where channel is not null
      and customer_id is null
),
enriched as (
    -- Attach customers attributes
    select a.*, p.age
    from valid a
    left join customers p
        on a.customer_id = p.id
),
scored as (
    -- Ranking and gap features
    select
        *,
        sum(amount) over w_grp as cumulative_amount,
        ntile(3) over w_recent as shipping_fee_bucket,
        ntile(5) over w_hist as amount_bucket
    from enriched
    window w_grp as (partition by status order by age, id rows between unbounded preceding and current row), w_recent as (partition by store_id order by shipping_fee, id), w_hist as (partition by customer_id order by amount, id)
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        cume_dist() over (order by amount_bucket) as amount_bucket_cume_dist,
        ntile(10) over (partition by channel, store_id order by amount_bucket, id) as amount_bucket_bucket
    from scored
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        status,
        amount,
        amount_bucket_cume_dist
    from metrics
) src
unpivot include nulls (
    val for metric_name in (amount, amount_bucket_cume_dist as AMOUNT_BUCKET_CUME_DIST)
);
