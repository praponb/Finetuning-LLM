select id, method, paid_at, ntile(10) over (partition by method order by paid_at, id) as paid_at_ntile from payments;
