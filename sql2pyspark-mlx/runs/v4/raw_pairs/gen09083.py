from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount")
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("status", "method")
w_ordered = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id", "status").orderBy("amount", "id")
w6 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w7 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status", "order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("method").orderBy("amount_rank", "id")

base = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-06-18"))) & F.col("method").isNotNull())
    .withColumn("amount_level", F.when(F.col("amount") >= 1540, "High").when(F.col("amount") >= 510, "Medium").otherwise("Low"))
    .withColumn("method_filled", F.coalesce("method", F.lit("N/A")))
)

enriched = (
    base
    .select(
        "*",
        F.rank().over(w).alias("amount_rank"),
        F.ntile(3).over(w2).alias("amount_bucket"),
        F.sum("amount").over(w3).alias("status_sum_amount"),
        F.first("amount", ignorenulls=True).over(w_ordered).alias("first_known_amount"),
        F.ntile(3).over(w5).alias("amount_bucket_2"),
    )
)

gaps = (
    enriched
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w6) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w7))

result = (
    islands
    .withColumn("first_known_amount_rank", F.first("amount_rank", ignorenulls=True).over(w8))
    .withColumn("row_seq", F.row_number().over(w9))
)
