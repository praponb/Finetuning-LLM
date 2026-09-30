select * from (select is_active, segment, age from customers) pivot (avg(age) for segment in ('Corporate', 'SMB')) order by is_active;
