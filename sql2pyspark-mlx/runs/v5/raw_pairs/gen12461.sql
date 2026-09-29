with enriched as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        last_value(country) ignore nulls over (order by started_at, id rows between unbounded preceding and current row) as last_known_country
    from sessions
    where customer_id = 11
)
select
    s.id,
    s.customer_id,
    s.device,
    s.started_at,
    s.last_known_country,
    (select sum(t2.balance) from accounts t2 where t2.customer_id = s.customer_id) as sum_account_balance
from enriched s
