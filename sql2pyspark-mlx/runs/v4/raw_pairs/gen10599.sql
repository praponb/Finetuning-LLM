select s.*
from suppliers s
where not exists (
        select 1
        from products ch
        where ch.supplier_id = s.id
          and ch.launch_date > '2024-01-01'
    );
