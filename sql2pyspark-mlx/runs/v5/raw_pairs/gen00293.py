from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount_whole").asc_nulls_last(), "id")
w_recent = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("amount") < 1220) & (F.col("paid_at") >= F.lit("2023-05-02 03:00:00").cast("timestamp")))
    .select("*", F.floor("amount").alias("amount_whole"))
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

filtered_payments = (
    deduped
    .filter(F.col("order_id").isin([12, 35, 36, 37]))
    .select("*", F.concat_ws("/", "status", "method").alias("status_method_key"))
)

metrics = (
    filtered_payments
    .withColumn("running_sum_amount", F.sum("amount").over(w_recent))
    .withColumn("running_count_amount", F.count("amount").over(w_recent))
    .withColumn("running_min_amount", F.min("amount").over(w_recent))
)

filtered_payments_2 = (
    metrics
    .filter((F.col("paid_at") > F.lit("2023-11-03").cast("date")) & F.col("method").isNotNull())
    .select(
        "*",
        F.when(F.col("method").isin(["paypal", "bank_transfer"]), "Group A").otherwise("Other").alias("method_group"),
        F.length("status").alias("status_length"),
    )
)

result = (
    filtered_payments_2
    .groupBy(F.date_trunc("YEAR", "paid_at").alias("year_start"))
    .agg(
        F.max("paid_at").alias("ended_at"),
        F.min("paid_at").alias("first_seen"),
        F.percentile_approx("running_count_amount", [0.25, 0.5, 0.75]).alias("running_count_amount_quartiles"),
    )
    .orderBy(F.col("year_start").desc())
)
