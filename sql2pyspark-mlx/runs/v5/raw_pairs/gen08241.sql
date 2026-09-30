SELECT s.*
FROM products s
WHERE EXISTS (
        SELECT 1
        FROM inventory ch
        WHERE ch.product_id = s.id
          AND ch.last_restock <= s.launch_date
    );
