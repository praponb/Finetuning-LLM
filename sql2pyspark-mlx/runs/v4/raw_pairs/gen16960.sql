with valid as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        started_at,
        country,
        concat_ws('-', browser, country) browser_country_key,
        coalesce(browser, 'UNKNOWN') browser_clean
    from sessions
    where browser is not null
),
matched_keys as (
    -- Set difference on keys
    select customer_id, country
    from valid
    except distinct
    select customer_id, country
    from sessions
    where browser = 'Chrome'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    join matched_keys k on s.customer_id = k.customer_id and s.country = k.country
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        date_format(started_at, 'yyyy-MM-dd') started_at_label,
        coalesce(country, 'OTHER') resolved_country
    from reconciled
    where country is not null
      and started_at < '2024-09-27'
),
enriched as (
    -- Attach customers attributes
    select e.*, c.age
    from base e
    left join customers c
        on e.customer_id = c.id
),
with_dims as (
    -- Attach customers attributes
    select
        src.*,
        d.country as customer_country,
        d.email,
        d.age as customer_age
    from enriched src
    left join customers d
        on src.customer_id = d.id
)
-- Native SQL PIVOT
select *
from (
    select
        customer_id,
        customer_country,
        country,
        customer_age
    from with_dims
) src
pivot (
    sum(customer_age) as value_sum,
    count(1) as item_count
    for country in ('DE', 'UK')
);
