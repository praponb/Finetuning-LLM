with valid as (
    select *, concat_ws('-', country, browser) as country_browser_key
    from sessions
    where country = 'TH'
)
select
    *,
    last_value(browser, true) over w_running as last_known_browser,
    last_value(browser, true) over (partition by customer_id, browser order by started_at, id rows between unbounded preceding and current row) as last_known_browser_2,
    last_value(country) ignore nulls over (partition by country order by started_at, id rows between unbounded preceding and current row) as last_known_country,
    first_value(country) ignore nulls over (partition by browser, device order by started_at, id rows between unbounded preceding and current row) as first_known_country
from valid
window w_running as (order by started_at, id rows between unbounded preceding and current row)
