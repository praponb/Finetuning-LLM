select id, email, segment from customers where segment in ('Corporate', 'Consumer') union select id, email, segment from customers where city = 'Singapore' order by id desc;
