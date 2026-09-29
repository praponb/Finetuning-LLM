SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) rn FROM payments WHERE method <> 'bank_transfer') r WHERE rn = 1 ORDER BY method ASC;
