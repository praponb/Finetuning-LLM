WITH lines AS (
    SELECT
        id,
        customer_id,
        EXPLODE_OUTER(coupon_codes) AS code
    FROM carts
),
cleaned AS (
    SELECT
        *,
        COALESCE(code, 'Unknown') code_filled,
        UPPER(code) code_upper,
        COALESCE(code, 'OTHER') code_clean
    FROM lines
    WHERE customer_id IS NOT NULL
),
enriched AS (
    SELECT
        a.id,
        a.customer_id,
        a.code,
        a.code_filled,
        a.code_upper,
        a.code_clean,
        d.credit_limit
    FROM cleaned a
    INNER JOIN customers d
        ON a.customer_id = d.id
),
joined AS (
    SELECT
        a.*,
        c.age,
        c.name
    FROM enriched a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
cleaned_carts AS (
    SELECT
        id,
        customer_id,
        code_filled,
        code_upper,
        code_clean,
        credit_limit,
        age,
        CONCAT(name, '/', code) name_code
    FROM joined
    WHERE customer_id IS NOT NULL
),
grouped AS (
    SELECT
        CASE WHEN age >= 58 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
        customer_id,
        SUM(credit_limit) AS total_credit_limit,
        ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
        COUNT(*) AS row_count
    FROM cleaned_carts
    GROUP BY 1, 2
)
SELECT
    s.age_tier,
    s.customer_id,
    s.avg_credit_limit,
    s.row_count,
    (SELECT MIN(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS min_account_balance
FROM grouped s;
