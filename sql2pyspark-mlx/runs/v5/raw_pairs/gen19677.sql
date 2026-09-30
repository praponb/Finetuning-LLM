select id, browser, initcap(trim(browser)) as browser_title, datediff(current_date(), started_at) as days_since_started_at from sessions order by 3 asc, id asc limit 3;
