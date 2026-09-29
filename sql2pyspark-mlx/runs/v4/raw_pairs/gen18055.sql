with ranked as (
    select *, row_number() over (partition by customer_id order by balance desc nulls last, id asc) as dedup_rank
    from accounts
    qualify dedup_rank = 1
),
scored as (
    select
        *,
        first_value(balance) ignore nulls over (partition by status order by balance asc, id asc rows between unbounded preceding and current row) as first_known_balance,
        lag(balance, 1, 0) over (partition by account_type order by dedup_rank asc, id asc) as prev_balance,
        dense_rank() over (order by balance desc nulls last) as balance_dense_rank
    from ranked
),
cleaned as (
    select
        id,
        customer_id,
        balance,
        opened_date,
        status,
        first_known_balance,
        prev_balance,
        balance_dense_rank,
        round(dedup_rank / balance_dense_rank, 2) as dedup_rank_per_balance_dense_rank,
        coalesce(status, 'OTHER') as status_filled
    from scored
    where opened_date >= date '2023-07-12'
      and dedup_rank <= 1
),
not_flagged as (
    select s.*
    from cleaned s
    where not exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.txn_time <= s.opened_date
        )
),
enriched as (
    select
        *,
        avg(balance_dense_rank) over (partition by status order by first_known_balance asc, id asc rows between 5 preceding and 1 preceding) as balance_dense_rank_avg_5_prior,
        avg(prev_balance) over (order by first_known_balance asc, id asc rows between 3 preceding and 1 preceding) as prev_balance_avg_3_prior,
        ntile(5) over (partition by status order by first_known_balance asc, id asc) as first_known_balance_bucket
    from not_flagged
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by status order by balance_dense_rank asc nulls first, id asc) as rn
        from enriched
    ) x
    where rn = 1
)
select *
from (
    select
        customer_id,
        status,
        first_known_balance_bucket
    from deduped
) src
pivot (
    sum(first_known_balance_bucket)
    for status in ('FROZEN' as frozen, 'CLOSED' as closed)
);
