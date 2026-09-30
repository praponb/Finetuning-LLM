select id, age, name from customers where email like '%@company.co' union all select id, age, name from customers where lower(email) like '%ran%' order by id
