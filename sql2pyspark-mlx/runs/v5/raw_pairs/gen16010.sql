with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        upper(browser) as browser_upper,
        coalesce(country, 'DIRECT') as country_clean
    from sessions
    where country is not null
      and browser is distinct from 'Firefox'
),
enriched as (
    -- Attach customers attributes
    select
        s.*,
        p.country as customer_country,
        p.city,
        p.signup_date
    from cleaned s
    left join customers p
        on s.customer_id <=> p.id
),
enriched_sessions as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.browser,
        a.country,
        a.browser_upper,
        a.city,
        a.signup_date,
        b.country as customer_country_2
    from enriched a
    inner join customers b
        on a.customer_id = b.id
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by country order by signup_date desc, id) as event_seq,
        last_value(country, true) over (partition by customer_country_2 order by signup_date, id rows between unbounded preceding and current row) as last_known_country,
        first_value(customer_country_2) ignore nulls over (partition by customer_country_2 order by signup_date, id rows between unbounded preceding and current row) as first_known_customer_country_2
    from enriched_sessions
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            city,
            country,
            customer_country_2,
            event_seq
        from metrics
    ) src
    pivot (
        sum(event_seq)
        for customer_country_2 in ('TH' as th, 'US' as us, 'DE' as de)
    )
)
select
    city,
    country,
    th,
    de,
    case when city rlike '^(New York|Tokyo)$' then 'EXTENDED' else 'GROUP_B' end as city_zone
from by_type
order by city
