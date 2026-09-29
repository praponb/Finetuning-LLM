select id, amount from transactions where amount > (select avg(amount) from transactions);
