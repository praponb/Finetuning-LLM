select
    id,
    browser,
    case when device = 'tablet' then 1 else 0 end as is_tablet,
    coalesce(browser, 'Unknown') as browser_filled
from sessions
