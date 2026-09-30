select id, email from customers where credit_limit < 8300 except select id, email from customers where country in ('TH', 'SG')
