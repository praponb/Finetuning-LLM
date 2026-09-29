select sum(case when city = 'London' then sqft else 0 end) as london_sqft from stores where region not in ('North', 'East') and opened_date <= '2023-05-01' order by london_sqft asc;
