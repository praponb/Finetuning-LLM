-- Q19: Time-series gap filling and interpolation
-- needs setup views: price_history
-- @id: Q19
WITH spine AS (
  SELECT p.product_id, d
  FROM (SELECT DISTINCT product_id FROM price_history) p
  LATERAL VIEW explode(sequence(DATE'2024-01-01', DATE'2024-01-31', INTERVAL 1 DAY)) s AS d
),
joined AS (
  SELECT s.product_id, s.d, ph.price
  FROM spine s
  LEFT JOIN price_history ph ON ph.product_id = s.product_id AND ph.price_date = s.d
),
filled AS (
  SELECT
    *,
    -- two spellings of IGNORE NULLS on purpose
    last_value(price, true) OVER w_past                                            AS ffill,
    FIRST_VALUE(price) IGNORE NULLS OVER w_future                                  AS bfill,
    last_value(CASE WHEN price IS NOT NULL THEN d END, true) OVER w_past           AS prev_known_d,
    first_value(CASE WHEN price IS NOT NULL THEN d END, true) OVER w_future        AS next_known_d
  FROM joined
  WINDOW
    w_past   AS (PARTITION BY product_id ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    w_future AS (PARTITION BY product_id ORDER BY d ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
),
interp AS (
  SELECT
    *,
    CASE
      WHEN price IS NOT NULL                          THEN CAST(price AS DECIMAL(12,4))
      WHEN prev_known_d IS NULL OR next_known_d IS NULL THEN NULL
      ELSE CAST(ffill + (bfill - ffill) * datediff(d, prev_known_d) / datediff(next_known_d, prev_known_d)
                AS DECIMAL(12,4))
    END AS interp_price
  FROM filled
),
final AS (
  SELECT
    *,
    CAST(coalesce(interp_price, ffill, bfill) AS DECIMAL(12,4))                      AS price_filled,
    CASE
      WHEN price IS NOT NULL       THEN 'observed'
      WHEN interp_price IS NOT NULL THEN 'interpolated'
      WHEN ffill IS NOT NULL       THEN 'forward_filled'
      WHEN bfill IS NOT NULL       THEN 'back_filled'
      ELSE 'missing'
    END                                                                              AS fill_method
  FROM interp
)
SELECT
  product_id,
  d,
  price,
  ffill,
  bfill,
  interp_price,
  price_filled,
  fill_method,
  -- dense spine: ROWS and RANGE agree here...
  round(AVG(price_filled) OVER (PARTITION BY product_id ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 4) AS ma7_rows,
  round(AVG(price_filled) OVER (PARTITION BY product_id ORDER BY d
                                RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW), 4)                  AS ma7_range,
  -- ...but on the raw sparse observations they don't (NULL prices are ignored by AVG, not by the frame)
  round(AVG(price) OVER (PARTITION BY product_id ORDER BY d
                         RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW), 4)                         AS ma7_observed_only,
  price_filled - LAG(price_filled, 7) OVER (PARTITION BY product_id ORDER BY d)                               AS wow_change,
  round(100 * (price_filled / LAG(price_filled) OVER (PARTITION BY product_id ORDER BY d) - 1), 3)            AS dod_pct,
  datediff(d, prev_known_d)                                                                                   AS days_since_observation,
  MIN(price_filled) OVER (PARTITION BY product_id)                                                            AS month_low,
  -- struct ordering breaks ties (401 has a flat price all month): earliest low day wins
  d = min_by(d, struct(price_filled, d)) OVER (PARTITION BY product_id)                                      AS is_low_day
FROM final
ORDER BY product_id, d;
