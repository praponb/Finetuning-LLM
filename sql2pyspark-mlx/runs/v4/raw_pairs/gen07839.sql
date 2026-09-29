select
    id,
    device,
    started_at,
    browser,
    first_value(browser) over (partition by device order by started_at, id) as first_browser
from sessions
