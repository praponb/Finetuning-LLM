from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount_whole").desc_nulls_last(), "id")
w2 = Window.partitionBy("method", "order_id").orderBy("amount", "id")
w3 = Window.orderBy("adjusted_amount", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("adjusted_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter(F.col("paid_at") >= F.to_timestamp(F.lit("2023-05-12 11:00:00")))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.floor("amount").alias("amount_whole"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "method", "amount")
)

cleaned = (
    deduped
    .filter(F.col("order_id").isNotNull())
    .withColumn("adjusted_amount", F.round((F.col("amount") * 1.07), 2))
)

with_windows = (
    cleaned
    .withColumn("next_amount", F.lead("amount").over(w2))
    .withColumn("prev_amount", F.lag("amount").over(w2))
    .withColumn("next_adjusted_amount", F.lead("adjusted_amount").over(w2))
    .withColumn("row_seq", F.row_number().over(w2))
)

enriched = (
    with_windows
    .withColumn("row_seq_avg_5", F.avg("row_seq").over(w3))
    .withColumn("last_known_prev_amount", F.last("prev_amount", ignorenulls=True).over(w4))
)

grouped = (
    enriched
    .cube(
        "order_id",
        F.when(F.col("adjusted_amount") >= 67, "PREMIUM").otherwise("STANDARD").alias("adjusted_amount_tier"),
        F.when(F.col("amount") >= 1650, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.countDistinct("method").alias("distinct_methods"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
    )
)

result = grouped.select("amount_tier", "distinct_methods", "paypal_count")
