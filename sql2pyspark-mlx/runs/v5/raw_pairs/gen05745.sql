with with_windows as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        account_type,
        opened_date,
        status,
        sum(balance) over (order by opened_date, id rows between unbounded preceding and current row) cumulative_balance,
        ntile(4) over (order by balance desc, id) balance_bucket
    from accounts
    where status = 'CLOSED'
),
with_dims as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.account_type,
        a.opened_date,
        a.status,
        p.credit_limit,
        p.name
    from with_windows a
    left join customers p
        on a.customer_id = p.id
),
joined as (
    -- Attach customers attributes
    select
        s.*,
        c.country,
        c.name as customer_name
    from with_dims s
    left join customers c
        on s.customer_id = c.id
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from joined s
    where not exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and ch.txn_time >= s.opened_date
        )
)
-- Window metrics over several different partitions
select
    id,
    customer_id,
    credit_limit,
    name,
    country,
    customer_name,
    last_value(credit_limit, true) over (partition by account_type order by credit_limit, id rows between unbounded preceding and current row) last_known_credit_limit,
    row_number() over (partition by customer_id order by credit_limit desc, id) event_seq,
    rank() over (order by credit_limit desc nulls first) credit_limit_rank,
    avg(credit_limit) over (partition by country order by opened_date, id rows between 6 preceding and 1 preceding) credit_limit_avg_6_prior
from eligible;
