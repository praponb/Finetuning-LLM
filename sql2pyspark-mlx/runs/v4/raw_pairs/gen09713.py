from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan")
w_running = Window.orderBy(F.col("monthly_fee").desc_nulls_first())
w3 = Window.orderBy(F.col("monthly_fee").asc_nulls_first())
w4 = Window.orderBy("start_date_plus_months", "id")
w5 = Window.partitionBy("plan").orderBy("end_date", "id")
w6 = Window.partitionBy("customer_id")
w7 = Window.partitionBy("plan").orderBy(F.col("customer_id_min_monthly_fee_rank").desc(), "id")

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-11-17"))) & F.col("customer_id").isNotNull())
    .select("*", F.add_months("start_date", 12).alias("start_date_plus_months"))
)

enriched = (
    cleaned
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
    .withColumn("monthly_fee_rank", F.rank().over(w_running))
    .withColumn("monthly_fee_cume_dist", F.cume_dist().over(w3))
)

metrics = (
    enriched
    .select(
        "*",
        F.lag("monthly_fee_share", 1).over(w4).alias("prev_monthly_fee_share"),
        F.row_number().over(w5).alias("event_seq"),
        F.min("monthly_fee_rank").over(w6).alias("customer_id_min_monthly_fee_rank"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

enriched_subscriptions = (
    latest
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.signup_date"), F.col("d.email"), F.col("d.credit_limit"))
)

with_dims = (
    enriched_subscriptions
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select("e.*", F.col("c.segment"), F.col("c.email").alias("customer_email"))
)

result = (
    with_dims
    .groupBy("customer_id")
    .pivot("segment", ["SMB", "Consumer"])
    .agg(F.avg("monthly_fee_cume_dist").alias("total"), F.count(F.lit(1)).alias("n"))
    .select(
        "customer_id",
        F.col("SMB_total").alias("smb_total"),
        F.col("SMB_n").alias("smb_n"),
        F.col("Consumer_total").alias("consumer_total"),
        F.col("Consumer_n").alias("consumer_n"),
    )
)
