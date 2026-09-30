from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc(), "id")
w2 = Window.orderBy(F.col("unit_price").desc(), "id")
w3 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("row_seq_tier").orderBy(F.col("avg_quantity_bucket").desc())

enriched = (
    df_order_items
    .select(
        "*",
        F.ntile(5).over(w).alias("quantity_bucket"),
        F.row_number().over(w2).alias("row_seq"),
        F.first("unit_price", ignorenulls=True).over(w3).alias("first_known_unit_price"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("row_seq") >= 9, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        "order_id",
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("quantity_bucket"), 2).alias("avg_quantity_bucket"),
    )
    .withColumn("avg_quantity_bucket_dense_rank", F.dense_rank().over(w4))
)
