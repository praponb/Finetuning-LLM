with filtered as (
    select
        *,
        round(amount * 1.15, 2) as amount_scaled,
        concat_ws('-', method, status) as method_status_key
    from payments
    where paid_at >= timestamp '2023-05-16 09:00:00'
),
joined as (
    select a.*, b.discount
    from filtered a
    inner join orders b
        on a.order_id = b.id
),
remaining_keys as (
    select order_id, id
    from joined
    except all
    select order_id, id
    from payments
    where order_id in (3, 9, 21, 37)
),
reconciled as (
    select s.*
    from joined s
    inner join remaining_keys k on s.order_id = k.order_id and s.id = k.id
),
scored as (
    select
        *,
        dense_rank() over (partition by order_id order by amount_scaled nulls first) as amount_scaled_dense_rank,
        avg(discount) over (partition by method order by cast(paid_at as bigint) range between 2592000 preceding and current row) as discount_avg_30d,
        count(amount) over (order by cast(paid_at as bigint) range between 604800 preceding and current row) as amount_count_7d
    from reconciled
),
metrics as (
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_scaled,
        method_status_key,
        discount,
        amount_scaled_dense_rank,
        discount_avg_30d,
        amount_count_7d,
        sum(amount_scaled_dense_rank) over (partition by status) as status_sum_amount_scaled_dense_rank,
        round(amount_scaled_dense_rank / sum(amount_scaled_dense_rank) over (partition by order_id), 4) as amount_scaled_dense_rank_share,
        round(amount / sum(amount) over (partition by status), 4) as amount_share,
        avg(amount_count_7d) over w_hist as amount_count_7d_avg_90d,
        sum(amount_scaled) over (partition by status, order_id) as status_sum_amount_scaled
    from scored
    window w_hist as (partition by method order by cast(paid_at as bigint) range between 7776000 preceding and current row)
),
enriched as (
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_scaled,
        method_status_key,
        discount,
        amount_scaled_dense_rank,
        discount_avg_30d,
        amount_count_7d,
        status_sum_amount_scaled_dense_rank,
        amount_scaled_dense_rank_share,
        amount_share,
        amount_count_7d_avg_90d,
        status_sum_amount_scaled,
        sum(amount) over (partition by status, order_id) as status_sum_amount,
        avg(status_sum_amount_scaled_dense_rank) over (partition by status, method order by cast(paid_at as bigint) range between 604800 preceding and current row) as status_sum_amount_scaled_dense_rank_avg_7d
    from metrics
),
valid as (
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        method_status_key,
        discount,
        amount_scaled_dense_rank,
        discount_avg_30d,
        amount_count_7d,
        amount_share,
        status_sum_amount_scaled,
        status_sum_amount,
        status_sum_amount_scaled_dense_rank_avg_7d,
        cast(status_sum_amount_scaled as string) as status_sum_amount_scaled_num,
        case when status_sum_amount >= 6270 then 'High' when status_sum_amount >= 2876 then 'Medium' else 'Low' end as status_sum_amount_band,
        coalesce(method, 'UNKNOWN') as method_filled
    from enriched
    where paid_at >= timestamp '2024-01-08 23:00:00'
      and method is not null
)
select
    case when amount >= 1330 then 'LARGE' else 'SMALL' end as amount_tier,
    status,
    case when status_sum_amount >= 6413 then 'PREMIUM' else 'STANDARD' end as status_sum_amount_tier,
    round(avg(status_sum_amount_scaled_dense_rank_avg_7d), 2) as avg_status_sum_amount_scaled_dense_rank_avg_7d,
    min(paid_at) as started_at,
    min(paid_at) as started_at_2,
    sum(case when status = 'FAILED' then status_sum_amount else 0 end) as failed_status_sum_amount,
    size(collect_set(status)) as num_status,
    count(case when status = 'SUCCESS' then 1 end) as success_count
from valid
group by amount_tier, status, status_sum_amount_tier
order by status
