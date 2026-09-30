with filtered as (
    select
        id,
        order_id,
        method,
        amount,
        status,
        ceil(amount) amount_whole,
        coalesce(method, 'OTHER') resolved_method
    from payments
    where amount >= 1210
),
matched_keys as (
    select order_id, method
    from filtered
    intersect all
    select order_id, method
    from payments
    where order_id = 6
),
kept_rows as (
    select s.*
    from filtered s
    join matched_keys k on s.order_id = k.order_id and s.method = k.method
),
with_windows as (
    select
        id,
        status,
        resolved_method,
        dense_rank() over (order by amount_whole desc) amount_whole_dense_rank,
        round(amount / sum(amount) over (partition by status), 4) amount_share,
        cume_dist() over (order by amount) amount_cume_dist
    from kept_rows
),
with_windows_payments as (
    select
        id,
        amount_share,
        count(amount_share) over (partition by status) status_count_amount_share
    from with_windows
),
cleaned as (
    select *, amount_share * status_count_amount_share amount_share_times_status_count_amount_share
    from with_windows_payments
    where amount_share < 0.33
),
enriched as (
    select
        id,
        status_count_amount_share,
        amount_share_times_status_count_amount_share,
        percent_rank() over w_hist status_count_amount_share_percent_rank,
        cume_dist() over w_part amount_share_times_status_count_amount_share_cume_dist
    from cleaned
    window w_hist as (order by status_count_amount_share nulls last), w_part as (order by amount_share_times_status_count_amount_share nulls first)
)
select
    case when status_count_amount_share >= 17 then 'LARGE' else 'SMALL' end as status_count_amount_share_tier,
    case when amount_share_times_status_count_amount_share >= 75 then 'PREMIUM' else 'STANDARD' end as amount_share_times_status_count_amount_share_tier,
    avg(amount_share_times_status_count_amount_share) as avg_amount_share_times_status_count_amount_share,
    sum(amount_share_times_status_count_amount_share_cume_dist) as total_amount_share_times_status_count_amount_share_cume_dist,
    count(distinct id) as distinct_ids,
    grouping(case when status_count_amount_share >= 17 then 'LARGE' else 'SMALL' end) as gid
from enriched
group by grouping sets ((case when status_count_amount_share >= 17 then 'LARGE' else 'SMALL' end, case when amount_share_times_status_count_amount_share >= 75 then 'PREMIUM' else 'STANDARD' end), (case when status_count_amount_share >= 17 then 'LARGE' else 'SMALL' end), ());
