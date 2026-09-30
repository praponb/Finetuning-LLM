select city, sum(age) as total_age from customers where (age = 36 and name like '%Sri%') or lower(name) like '%liv%' group by city
