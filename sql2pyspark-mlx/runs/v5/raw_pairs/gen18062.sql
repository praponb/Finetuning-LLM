with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        account_type,
        opened_date,
        date_trunc('WEEK', opened_date) as opened_date_period,
        coalesce(status, 'NONE') as status_filled
    from accounts
    where opened_date >= date '2023-07-03'
      and status is not null
),
ranked as (
    -- Keep one row per key (latest first)
    select
        id,
        customer_id,
        account_type,
        opened_date_period,
        status_filled
    from (
        select *, row_number() over (partition by account_type order by opened_date_period desc, id) as row_num
        from cleaned
    ) t
    where row_num = 1
),
latest_match as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select s.*, latest.transaction_txn_type
    from ranked s
    left join lateral (
            select c.txn_type as transaction_txn_type
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) latest on true
),
remaining_keys as (
    -- Set difference on keys
    select account_type, id
    from latest_match
    except
    select account_type, id
    from accounts
    where account_type in ('CHECKING', 'LOAN', 'CREDIT')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest_match s
    inner join remaining_keys k on s.account_type = k.account_type and s.id = k.id
),
enriched as (
    -- Attach customers attributes
    select
        e.*,
        p.country,
        p.credit_limit
    from verified e
    inner join customers p
        on e.customer_id = p.id
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_time < s.opened_date_period
        )
),
enriched_accounts as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by customer_id, country order by credit_limit nulls first) as credit_limit_percent_rank,
        count(credit_limit) over (order by cast(opened_date_period as bigint) range between 604800 preceding and current row) as credit_limit_count_7d,
        max(credit_limit) over (order by cast(opened_date_period as bigint) range between 7776000 preceding and current row) as credit_limit_max_90d,
        sum(credit_limit) over (order by cast(opened_date_period as bigint) range between 604800 preceding and current row) as credit_limit_sum_7d,
        dense_rank() over (partition by transaction_txn_type order by credit_limit nulls first) as credit_limit_dense_rank
    from not_flagged
)
-- Grouped metrics
select
    case when credit_limit_percent_rank >= 0.87 then 'PREMIUM' else 'STANDARD' end as credit_limit_percent_rank_tier,
    date_trunc('quarter', opened_date_period) as quarter_start,
    case when credit_limit >= 13900 then 'HIGH' else 'LOW' end as credit_limit_tier,
    count(*) as num_rows,
    count(case when transaction_txn_type = 'CREDIT' then 1 end) as credit_count,
    approx_percentile(credit_limit_max_90d, array(0.25, 0.5, 0.75)) as credit_limit_max_90d_quartiles
from enriched_accounts
group by case when credit_limit_percent_rank >= 0.87 then 'PREMIUM' else 'STANDARD' end, date_trunc('quarter', opened_date_period), case when credit_limit >= 13900 then 'HIGH' else 'LOW' end
order by credit_limit_percent_rank_tier nulls first;
