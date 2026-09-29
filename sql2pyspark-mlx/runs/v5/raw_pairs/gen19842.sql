select country, case when browser = 'Firefox' then 1 else 0 end as is_firefox, to_date(started_at) as started_at_date from sessions where customer_id in (5, 30, 33)
