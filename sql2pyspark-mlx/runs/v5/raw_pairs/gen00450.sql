select customer_id, browser, initcap(trim(device)) as device_title, lower(browser) as browser_lower from sessions where country is not null order by customer_id asc
