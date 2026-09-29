WITH valid AS (
    SELECT *, CONCAT_WS(',', status, plan) AS status_plan_key
    FROM subscriptions
    WHERE end_date > TO_DATE('2024-05-31')
)
SELECT plan
FROM valid
EXCEPT ALL
SELECT plan
FROM subscriptions
WHERE end_date <= '2023-07-30'
