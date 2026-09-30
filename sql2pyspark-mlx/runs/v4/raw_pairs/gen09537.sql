WITH latest AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC NULLS LAST, id) AS rn FROM accounts QUALIFY rn = 1) SELECT id, balance, status FROM latest;
