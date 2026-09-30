select customer_id, concat(browser, ' - ', device) as browser_device, coalesce(country, 'N/A') as country_filled from sessions order by country_filled, 2
