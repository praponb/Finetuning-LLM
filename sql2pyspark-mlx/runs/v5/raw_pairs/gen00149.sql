WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    id,
    plan,
    effective_from,
    currency,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY entry_price DESC, id) AS seq_no,
    MAX(entry_price) OVER w_recent AS currency_max_entry_price,
    ROW_NUMBER() OVER (ORDER BY entry_price, id) AS row_seq,
    AVG(entry_price) OVER (PARTITION BY plan ORDER BY entry_price, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS entry_price_avg_5_prior,
    LAST_VALUE(entry_price, TRUE) OVER (PARTITION BY currency ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_entry_price
FROM unpacked
WINDOW w_recent AS (PARTITION BY currency)
