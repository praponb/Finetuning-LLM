select id, status, paid_at, method from (select *, row_number() over (partition by status order by paid_at desc, id) as row_num from payments) as t where row_num = 1 order by id desc, status desc
