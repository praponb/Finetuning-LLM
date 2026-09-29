from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc(), "id")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"))
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.*", F.col("p.signup_date"), F.col("p.email"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("src.*", F.col("d.credit_limit"), F.col("d.country"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("monthly_fee_filled") >= 60, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_filled_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.sort_array(F.collect_set("plan")).alias("plan_list_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("plan") == "premium", 1)), F.lit(0)), 2).alias("credit_limit_per_premium"),
    )
    .orderBy(F.col("monthly_fee_filled_tier").desc())
)
