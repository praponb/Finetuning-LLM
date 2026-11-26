-- Q23b: Three-valued logic, null-aware aggregates and comparisons
-- needs setup views: customers
-- @id: Q23b
SELECT
  country_code,
  COUNT(*)                                                    AS rows_all,
  COUNT(email)                                                AS rows_with_email,
  COUNT(DISTINCT tier)                                        AS distinct_tiers_non_null,
  count_if(tier = 'GOLD')                                     AS gold,
  count_if(NOT (tier = 'GOLD'))                               AS not_gold_excl_null,
  count_if(tier IS DISTINCT FROM 'GOLD')                      AS not_gold_incl_null,
  count_if(tier = NULL)                                       AS eq_null_always_0,
  SUM(CASE WHEN tier IS NULL THEN 1 END)                      AS null_tier_sum_or_null,
  concat(min(full_name), ' / ', max(email))                   AS concat_nullable,
  concat_ws(' / ', min(full_name), max(email))                AS concat_ws_skips_null,
  greatest(MIN(signup_date), CAST(NULL AS DATE))              AS greatest_ignores_null,
  nvl2(MAX(email), 'has email', 'no email')                   AS nvl2_check,
  nullif(COUNT(email), COUNT(*))                              AS missing_email_marker,
  sort_array(collect_list(email))                             AS emails_no_nulls,
  AVG(size(interests))                                        AS avg_interests,
  AVG(CASE WHEN size(interests) > 0 THEN size(interests) END) AS avg_interests_non_empty,
  bool_and(email LIKE '%@%')                                  AS all_emails_valid_3vl
FROM customers
GROUP BY country_code
ORDER BY country_code;
