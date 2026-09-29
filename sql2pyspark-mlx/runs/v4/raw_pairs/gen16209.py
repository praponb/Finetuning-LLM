from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "browser").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("browser", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

enriched = (
    df_sessions
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w2))
    .withColumn("row_seq_2", F.row_number().over(w_ordered))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w4))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w5))
)

result = (
    enriched
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "last_known_country",
        "first_known_country",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "GROUP_A").otherwise("EXTENDED").alias("device_zone"),
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("device_zone").asc_nulls_first())
)
