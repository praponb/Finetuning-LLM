from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit")
w2 = Window.partitionBy("city").orderBy(F.col("signup_date").desc(), "id")
w3 = Window.partitionBy("segment", "city").orderBy(F.col("credit_limit").asc_nulls_first(), "id")

kept = (
    df_customers
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.discount") > 0.16), "left_anti")
)

ranked = (
    kept
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
    )
)

ranked_customers = (
    deduped
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

daily = (
    ranked_customers
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 12300, "LARGE").otherwise("SMALL"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "signup_date"))
    .groupingSets(
        [["credit_limit_tier", "quarter_start"], ["credit_limit_tier"]],
        "credit_limit_tier",
        "quarter_start",
    )
    .agg(
        F.max(F.when(F.col("country") == "DE", F.col("credit_limit"))).alias("de_max_credit_limit"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("credit_limit")).otherwise(0)).alias("smb_credit_limit"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("segment") == "SMB", 1)), F.lit(0)), 2).alias("credit_limit_per_smb"),
        F.percentile_approx("age", 0.95).alias("age_p95"),
        F.avg("age").alias("avg_age"),
    )
    .filter(F.col("de_max_credit_limit") < 1000000)
)

result = (
    daily
    .select(
        "credit_limit_tier",
        "quarter_start",
        "de_max_credit_limit",
        "smb_credit_limit",
        "berlin_rows",
        "age_p95",
        "avg_age",
    )
)
