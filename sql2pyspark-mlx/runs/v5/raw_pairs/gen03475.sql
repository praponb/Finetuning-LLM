with enriched as (
    -- Window metrics over several different partitions
    select
        id,
        account_type,
        opened_date,
        status,
        lead(balance, 1) over (partition by account_type order by opened_date, id) as next_balance,
        lead(balance, 1) over w_part as next_balance_2,
        sum(balance) over (partition by account_type order by balance, id rows between unbounded preceding and current row) as cumulative_balance
    from accounts
    where opened_date <= '2024-02-01'
    window w_part as (partition by status order by balance, id)
)
-- Grouped metrics
select
    case when next_balance >= 77000 then 'PREMIUM' else 'STANDARD' end as next_balance_tier,
    date_trunc('year', opened_date) as year_start,
    approx_percentile(next_balance, 0.5) as next_balance_p50,
    avg(next_balance_2) as avg_next_balance_2,
    percent_rank() over (partition by case when next_balance >= 77000 then 'PREMIUM' else 'STANDARD' end order by avg(next_balance_2) desc) as avg_next_balance_2_percent_rank
from enriched
group by next_balance_tier, year_start
