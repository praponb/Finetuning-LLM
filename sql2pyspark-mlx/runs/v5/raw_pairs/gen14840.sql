WITH base AS (SELECT *, LENGTH(plan) plan_length FROM subscriptions WHERE start_date > DATE '2023-06-23') SELECT id FROM base EXCEPT ALL SELECT id FROM subscriptions WHERE start_date >= '2025-10-16'
