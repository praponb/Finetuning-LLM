select country, count(case when city = 'Bangkok' then 1 end) as bangkok_count from suppliers group by country having bangkok_count < 3
