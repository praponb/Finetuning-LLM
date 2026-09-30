from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("segment", "city")
w2 = Window.partitionBy("city").orderBy(F.col("age").asc_nulls_last())
w_hist = Window.partitionBy("country", "city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.partitionBy("country").orderBy("signup_date", "id")
w5 = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

scored = (
    df_customers
    .select(
        "*",
        F.corr("age", "credit_limit").over(w_grp).alias("corr_age_credit_limit"),
        F.cume_dist().over(w2).alias("age_cume_dist"),
        F.last("segment", ignorenulls=True).over(w_hist).alias("last_known_segment"),
        F.lag("credit_limit", 2).over(w_hist_2).alias("prev_credit_limit"),
        F.avg("age").over(w5).alias("age_avg_6_prior"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.opened_date").alias("account_opened_date"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") <= F.col("s.signup_date")), "left_semi")
)

remaining_keys = (
    not_flagged
    .select("country", "segment")
    .subtract(
        df_customers
        .filter(F.col("signup_date") > "2024-05-01")
        .select("country", "segment")
    )
)

verified = (
    not_flagged
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.count_if(F.col("segment") == "Consumer").alias("consumer_events"),
        F.countDistinct("country").alias("distinct_countries"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("age_per_bangkok"),
    )
    .filter(F.col("corporate_count") >= 1)
    .orderBy(F.col("distinct_countries").asc_nulls_last())
)
