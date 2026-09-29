from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").asc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy(F.col("monthly_fee").desc_nulls_first())
w4 = Window.partitionBy("plan", "status").orderBy("monthly_fee", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w6 = Window.partitionBy("customer_id").orderBy("end_date", "id")
w7 = Window.partitionBy("plan", "customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w9 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("monthly_fee_rank", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-08-13").cast("date")) & F.col("status").isin(["ACTIVE", "EXPIRED"]))
    .select(
        "*",
        F.trim("status").alias("clean_status"),
        F.length("status").alias("status_length"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    latest
    .select(
        "*",
        F.last("monthly_fee", ignorenulls=True).over(w2).alias("last_known_monthly_fee"),
        F.rank().over(w_ordered).alias("monthly_fee_rank"),
        F.avg("monthly_fee").over(w4).alias("monthly_fee_avg_3_prior"),
    )
)

metrics = (
    enriched
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("first_known_status_length", F.first("status_length", ignorenulls=True).over(w7))
    .withColumn("row_seq_2", F.row_number().over(w8))
)

result = (
    metrics
    .withColumn("cumulative_first_known_status_length", F.sum("first_known_status_length").over(w9))
    .withColumn("status_length_avg_3", F.avg("status_length").over(w_recent))
)
