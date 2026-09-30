from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("amount_whole", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy(F.col("amount").desc(), "id")

cleaned = (
    df_payments
    .filter(~F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
    .select(
        "id",
        "amount",
        "paid_at",
        F.floor("amount").alias("amount_whole"),
        F.initcap(F.trim("method")).alias("method_title"),
    )
)

metrics = (
    cleaned
    .withColumn("running_amount", F.sum("amount").over(w))
    .withColumn("amount_whole_avg_5", F.avg("amount_whole").over(w2))
    .withColumn("first_known_amount_whole", F.first("amount_whole", ignorenulls=True).over(w))
    .withColumn("amount_bucket", F.ntile(4).over(w3))
)

valid = (
    metrics
    .filter(F.col("paid_at") >= F.lit("2023-05-27").cast("date"))
    .select(
        "id",
        "amount",
        "paid_at",
        "amount_whole",
        "method_title",
        "running_amount",
        "amount_whole_avg_5",
        "first_known_amount_whole",
        "amount_bucket",
        F.coalesce("amount_whole_avg_5", F.lit(0)).alias("amount_whole_avg_5_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.date_trunc("YEAR", "paid_at").alias("year_start"),
        F.when(F.col("amount_whole_avg_5_filled") >= 84, "PREMIUM").otherwise("STANDARD").alias("amount_whole_avg_5_filled_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("paid_at").alias("ended_at"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.max("paid_at").alias("ended_at_2"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
