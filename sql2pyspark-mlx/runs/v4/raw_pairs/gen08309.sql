SELECT id, opened_date, ROW_NUMBER() OVER (ORDER BY opened_date, id) opened_date_row
FROM stores
WHERE opened_date BETWEEN '2025-07-02' AND '2025-07-10'
