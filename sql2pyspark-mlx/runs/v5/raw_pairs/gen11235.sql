select a.id, a.status, a.discount, a.amount from orders a where a.discount < (select avg(b.discount) from orders b where b.status = a.status);
