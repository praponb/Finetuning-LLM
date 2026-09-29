from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit_filled").desc(), "id")

base = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.end_date") < F.col("s.signup_date")), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.signup_date")), "left_anti")
)

eligible_customers = (
    not_flagged
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") >= F.col("s.signup_date")), "left_anti")
)

latest = (
    eligible_customers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

pivoted = (
    latest
    .groupBy("country")
    .pivot("segment", ["Corporate", "Consumer", "SMB"])
    .agg(F.sum("credit_limit_filled").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "country",
        F.col("Corporate_amount").alias("corporate_amount"),
        F.col("Corporate_cnt").alias("corporate_cnt"),
        F.col("Consumer_amount").alias("consumer_amount"),
        F.col("Consumer_cnt").alias("consumer_cnt"),
        F.col("SMB_amount").alias("smb_amount"),
        F.col("SMB_cnt").alias("smb_cnt"),
    )
)

result = (
    pivoted
    .select("country", "corporate_amount", "corporate_cnt", "consumer_cnt", "smb_cnt")
    .orderBy(F.col("consumer_cnt").desc(), F.col("corporate_amount").desc())
)
