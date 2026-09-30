select s.id, count(p.id) as product_count, max(p.stock_qty) as max_stock_qty from suppliers s left join products p on s.id = p.supplier_id group by s.id;
