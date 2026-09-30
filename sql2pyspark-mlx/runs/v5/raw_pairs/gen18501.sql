with scored as (
    -- Ranking and gap features
    select
        *,
        row_number() over (partition by browser order by started_at desc, id) as row_seq,
        last_value(country, true) over (order by started_at, id rows between unbounded preceding and current row) as last_known_country
    from sessions
    where country = 'SG'
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from scored s
    where exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.event_time < s.started_at
        )
),
enriched as (
    -- Attach customers attributes
    select
        s.*,
        p.credit_limit,
        p.email,
        p.signup_date
    from not_flagged s
    left join customers p
        on s.customer_id <=> p.id
),
matched as (
    -- Non-equi join on a validity window
    select
        s.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from enriched s
    join subscriptions sub
        on s.customer_id = sub.customer_id
       and s.started_at >= sub.start_date
       and s.started_at < sub.end_date + interval '7' day
),
scored_sessions as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        device,
        country,
        last_known_country,
        signup_date,
        subscription_id,
        monthly_fee,
        lead(row_seq, 2) over (partition by device order by monthly_fee, id, subscription_id) as next_row_seq,
        last_value(browser, true) over (order by credit_limit, id, subscription_id rows between unbounded preceding and current row) as last_known_browser,
        avg(credit_limit) over w_grp as credit_limit_avg_5_prior
    from matched
    window w_grp as (partition by country order by row_seq, id, subscription_id rows between 5 preceding and 1 preceding)
)
-- Join a dimension and aggregate
select
    c.segment,
    c.country as customer_country,
    count(*) as rows_total,
    sum(src.subscription_id) as total_subscription_id
from scored_sessions src
join customers c on src.customer_id = c.id
group by c.segment, c.country
order by segment desc nulls first
