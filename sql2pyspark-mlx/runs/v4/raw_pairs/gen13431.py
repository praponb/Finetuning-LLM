from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")
w2 = Window.orderBy("age", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("country").orderBy("age", "id")
w4 = Window.partitionBy("country").orderBy(F.col("age_avg_2_prior").desc_nulls_first())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("country").orderBy("age_avg_2_prior_rounded", "id")
w7 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("segment").orderBy("discount", "id")
w9 = Window.partitionBy("city")
w10 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-06-13"))) & F.col("city").isNotNull())
    .select("*", F.coalesce("email", F.lit("N/A")).alias("email_filled"))
)

metrics = (
    filtered
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("age_avg_2_prior", F.avg("age").over(w2))
    .withColumn("next_credit_limit", F.lead("credit_limit", 2).over(w3))
)

latest = (
    metrics
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

valid = (
    latest
    .filter(~F.col("is_active"))
    .select("*", F.round("age_avg_2_prior", 0).alias("age_avg_2_prior_rounded"))
)

with_last = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("order_amount"), F.col("c.discount"))
)

metrics_customers = (
    with_last
    .withColumn("age_avg_2_prior_rounded_bucket", F.ntile(10).over(w6))
    .withColumn("last_known_segment", F.last("segment", ignorenulls=True).over(w7))
    .withColumn("discount_bucket", F.ntile(5).over(w8))
    .withColumn("next_credit_limit_share", F.round((F.col("next_credit_limit") / F.sum("next_credit_limit").over(w9)), 4))
    .withColumn("next_credit_limit_avg_6", F.avg("next_credit_limit").over(w10))
)

result = (
    metrics_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("activity_month"), "seq_no")
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("order_amount"))).alias("tokyo_total_order_amount"),
        F.round(F.avg("discount_bucket"), 2).alias("avg_discount_bucket"),
        F.round(F.sum("next_credit_limit_avg_6") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("next_credit_limit_avg_6_per_tokyo"),
    )
    .filter(F.col("next_credit_limit_avg_6_per_tokyo") >= 0)
)
