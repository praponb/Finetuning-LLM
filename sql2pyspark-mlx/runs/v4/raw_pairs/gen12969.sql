with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        length(country) as country_length,
        coalesce(browser, 'NONE') as resolved_browser
    from sessions
    where started_at >= timestamp '2023-09-15 20:00:00'
)
select customer_id, country
from filtered
except all
select customer_id, country
from sessions
where customer_id is not null;
