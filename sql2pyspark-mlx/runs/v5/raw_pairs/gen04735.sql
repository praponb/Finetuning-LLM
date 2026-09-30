SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) pos
    FROM payments
) ranked
WHERE pos <= 1
ORDER BY method ASC;
