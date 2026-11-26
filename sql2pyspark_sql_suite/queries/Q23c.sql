-- Q23c: Numeric typing: decimals, int division, rounding modes, try_* and ANSI casts
-- needs setup views: order_items
-- @id: Q23c
SELECT
  oi.order_id,
  oi.line_no,
  oi.unit_price,
  oi.quantity,
  oi.discount_pct,
  oi.unit_price / 3                                         AS dec_div,           -- decimal(..)
  typeof(oi.unit_price / 3)                                 AS t_dec_div,
  oi.quantity / 2                                           AS int_div_is_double,
  typeof(oi.quantity / 2)                                   AS t_int_div,
  oi.quantity div 2                                         AS int_div_floor,
  -oi.quantity % 3                                          AS neg_mod,
  pmod(-oi.quantity, 3)                                     AS pos_mod,
  round(oi.unit_price / 7, 2)                               AS round_half_up,
  bround(oi.unit_price / 8, 1)                              AS round_half_even,
  ceil(oi.unit_price / 7, 1)                                AS ceil_1dp,
  floor(-oi.unit_price / 7, -1)                             AS floor_tens,
  typeof(ceil(oi.unit_price / 7, 1))                        AS t_ceil,
  oi.quantity * oi.unit_price * (1 - oi.discount_pct)       AS net_exact,
  typeof(oi.quantity * oi.unit_price * (1 - oi.discount_pct)) AS t_net,
  CAST(oi.discount_pct AS FLOAT) + CAST(0.1 AS FLOAT)       AS float_sum,
  try_divide(oi.unit_price, oi.discount_pct)                AS price_per_disc,    -- NULL on /0
  try_multiply(2147483647, oi.quantity)                     AS int_mul_overflow,  -- NULL when > int max
  try_add(9223372036854775807L, CAST(oi.line_no - 1 AS BIGINT)) AS bigint_add_overflow,
  try_cast(concat(CAST(oi.quantity AS STRING), '.5') AS INT)  AS try_cast_frac,     -- NULL under ANSI
  CAST(concat(' ', CAST(oi.quantity AS STRING), ' ') AS INT)  AS cast_trims_spaces,
  CAST(oi.unit_price AS DECIMAL(10,0))                      AS narrowing_cast_rounds,
  format_number(oi.unit_price * 1000, 1)                    AS formatted,
  round(oi.unit_price, -2)                                  AS round_hundreds,
  shiftleft(oi.quantity, 3) | oi.line_no                    AS bit_mix,
  bit_count(oi.order_id)                                    AS bits_in_order_id
FROM order_items oi
ORDER BY oi.order_id, oi.line_no;
