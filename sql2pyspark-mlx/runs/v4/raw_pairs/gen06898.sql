WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        coupon_codes,
        items,
        ship_to,
        attributes,
        ship_to.zip AS ship_zip,
        ARRAY_CONTAINS(coupon_codes, 'WELCOME5') AS used_welcome5,
        AGGREGATE(items, 0D, (acc, li) -> acc + li.qty * li.price, acc -> ROUND(acc, 2)) AS basket_amount
    FROM carts
)
-- Window metrics over several different partitions
SELECT
    id,
    coupon_codes,
    items,
    ship_to,
    attributes,
    ship_zip,
    used_welcome5,
    basket_amount,
    AVG(basket_amount) OVER w_ordered AS basket_amount_avg_6,
    ROW_NUMBER() OVER (ORDER BY basket_amount DESC, id) AS row_seq,
    AVG(basket_amount) OVER (ORDER BY basket_amount, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS basket_amount_avg_5_prior
FROM parsed
WINDOW w_ordered AS (ORDER BY basket_amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);
