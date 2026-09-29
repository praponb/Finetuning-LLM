from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("method")
w3 = Window.orderBy("row_num", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.orderBy("paid_at", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("method_count_amount", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy("method_count_amount", "id")

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-05-18 19:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.to_date("paid_at").alias("paid_at_date"),
    )
)

ranked = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "paid_at_date",
        "row_num",
        F.count("amount").over(w2).alias("method_count_amount"),
        F.avg("amount").over(w3).alias("amount_avg_5"),
    )
)

base = (
    with_windows
    .filter(F.col("order_id").isNotNull() & (F.col("paid_at") > F.lit("2023-10-02").cast("date")))
    .select(
        "id",
        "amount",
        "paid_at",
        "row_num",
        "method_count_amount",
        "amount_avg_5",
        F.col("amount").cast("int").alias("amount_int"),
        F.least("amount", "amount_avg_5").alias("least_amount_amount_avg_5"),
    )
)

metrics = (
    base
    .select(
        "id",
        "paid_at",
        "method_count_amount",
        F.avg("amount_avg_5").over(w4).alias("amount_avg_5_avg_6_prior"),
        F.avg("least_amount_amount_avg_5").over(w5).alias("least_amount_amount_avg_5_avg_6"),
        F.lead("amount", 1).over(w6).alias("next_amount"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("next_amount") >= 1740, "PREMIUM").otherwise("STANDARD").alias("next_amount_tier"),
    )
    .agg(
        F.percentile_approx("next_amount", 0.95).alias("next_amount_p95"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("span_minutes") > 0)
)
