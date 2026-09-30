SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date DESC, id) AS latest_rank
    FROM accounts
) ranked
WHERE latest_rank = 1
