select id, method, paid_at, rank() over (partition by method order by paid_at desc) as paid_at_rank from payments
