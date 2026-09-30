select * from (select *, rank() over (partition by merchant order by txn_time desc) pos from transactions) ranked where pos = 1 order by amount desc, id desc;
