with base as (
    select
        *,
        case when method = 'card' then 1 else 0 end as is_card,
        trim(method) as clean_method
    from payments
    where paid_at >= timestamp '2023-08-30 06:00:00'
      and method = 'bank_transfer'
),
metrics as (
    select
        *,
        max(is_card) over (partition by status order by is_card, id rows between 3 preceding and current row) as last4_max_is_card,
        sum(is_card) over (partition by status order by is_card, id rows between 3 preceding and current row) as last4_sum_is_card,
        count(is_card) over (partition by status order by is_card, id rows between 3 preceding and current row) as last4_count_is_card,
        min(is_card) over (partition by status order by is_card, id rows between 3 preceding and current row) as last4_min_is_card,
        dense_rank() over (partition by method, status order by is_card desc) as is_card_dense_rank
    from base
),
flagged as (
    select *, case when paid_at - lag(paid_at) over (partition by order_id order by paid_at, id) > interval 60 days then 1 else 0 end as new_group_flag
    from metrics
),
sessionized as (
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        clean_method,
        last4_max_is_card,
        last4_count_is_card,
        last4_min_is_card,
        is_card_dense_rank,
        sum(new_group_flag) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as session_id
    from flagged
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by status order by last4_min_is_card desc nulls last, id) as dedup_rank
        from sessionized
    ) x
    where dedup_rank = 1
)
select *
from (
    select
        order_id,
        status,
        last4_count_is_card
    from deduped
) src
pivot (
    max(last4_count_is_card) as value_sum,
    count(1) as item_count
    for status in ('FAILED' as failed, 'SUCCESS' as success)
)
