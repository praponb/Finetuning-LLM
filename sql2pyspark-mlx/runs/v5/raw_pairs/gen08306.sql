with with_windows as (
    select
        *,
        count(quantity) over w_grp running_count_quantity,
        max(quantity) over w_grp running_max_quantity,
        min(quantity) over w_grp running_min_quantity,
        avg(quantity) over (partition by store_id order by quantity, id rows between 3 preceding and 1 preceding) quantity_avg_3_prior
    from inventory
    window w_grp as (partition by store_id, product_id order by quantity, id rows between unbounded preceding and current row)
),
latest as (
    select *
    from with_windows
    qualify row_number() over (partition by product_id order by running_max_quantity desc nulls last, id) = 1
),
enriched as (
    select
        id,
        store_id,
        quantity,
        running_count_quantity,
        running_min_quantity,
        quantity_avg_3_prior,
        avg(quantity_avg_3_prior) over w_hist last4_avg_quantity_avg_3_prior,
        max(quantity_avg_3_prior) over w_hist last4_max_quantity_avg_3_prior,
        min(quantity_avg_3_prior) over w_hist last4_min_quantity_avg_3_prior
    from latest
    window w_hist as (partition by product_id order by quantity, id rows between 3 preceding and current row)
),
with_windows_inventory as (
    select
        id,
        store_id,
        running_min_quantity,
        quantity_avg_3_prior,
        last4_avg_quantity_avg_3_prior,
        last4_max_quantity_avg_3_prior,
        sum(last4_min_quantity_avg_3_prior) over (partition by store_id order by quantity_avg_3_prior, id rows between unbounded preceding and current row) running_sum_last4_min_quantity_avg_3_prior,
        avg(last4_min_quantity_avg_3_prior) over (partition by store_id order by quantity_avg_3_prior, id rows between unbounded preceding and current row) running_avg_last4_min_quantity_avg_3_prior,
        max(last4_min_quantity_avg_3_prior) over (partition by store_id order by quantity_avg_3_prior, id rows between unbounded preceding and current row) running_max_last4_min_quantity_avg_3_prior
    from enriched
),
with_windows_inventory_2 as (
    select
        id,
        store_id,
        quantity_avg_3_prior,
        running_avg_last4_min_quantity_avg_3_prior,
        running_max_last4_min_quantity_avg_3_prior,
        ntile(5) over (partition by store_id order by running_sum_last4_min_quantity_avg_3_prior desc, id) running_sum_last4_min_quantity_avg_3_prior_bucket,
        avg(running_max_last4_min_quantity_avg_3_prior) over (partition by store_id order by last4_max_quantity_avg_3_prior, id rows between 6 preceding and current row) running_max_last4_min_quantity_avg_3_prior_avg_6,
        sum(last4_avg_quantity_avg_3_prior) over (partition by store_id order by last4_avg_quantity_avg_3_prior, id rows between unbounded preceding and current row) running_last4_avg_quantity_avg_3_prior
    from with_windows_inventory
    where running_max_last4_min_quantity_avg_3_prior >= 80
),
deduped as (
    select *
    from (
        select *, rank() over (partition by store_id order by running_max_last4_min_quantity_avg_3_prior_avg_6 desc nulls last) as dedup_rank
        from with_windows_inventory_2
    ) x
    where dedup_rank = 1
),
base as (
    select
        id,
        store_id,
        quantity_avg_3_prior,
        running_avg_last4_min_quantity_avg_3_prior,
        running_max_last4_min_quantity_avg_3_prior,
        running_last4_avg_quantity_avg_3_prior,
        dedup_rank,
        running_sum_last4_min_quantity_avg_3_prior_bucket - dedup_rank running_sum_last4_min_quantity_avg_3_prior_bucket_minus_dedup_rank
    from deduped
    where running_sum_last4_min_quantity_avg_3_prior_bucket < 6
)
select
    s.id,
    s.store_id,
    s.quantity_avg_3_prior,
    s.running_avg_last4_min_quantity_avg_3_prior,
    s.running_last4_avg_quantity_avg_3_prior,
    s.dedup_rank,
    (select max(t2.shipping_fee) from orders t2 where t2.store_id = s.store_id) as max_order_shipping_fee
from base s
order by running_last4_avg_quantity_avg_3_prior desc nulls first, quantity_avg_3_prior desc;
