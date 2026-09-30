WITH cleaned AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DATE_TRUNC('WEEK', opened_date) AS opened_date_period,
        DAYOFWEEK(opened_date) AS opened_date_dayofweek,
        COALESCE(status, 'NONE') AS resolved_status
    FROM accounts
    WHERE opened_date > TO_DATE('2023-08-19')
      AND customer_id IS NOT NULL
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date_period DESC, id) AS rank_in_group
FROM cleaned
QUALIFY rank_in_group = 1
