from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc_nulls_first(), "id")

deduped = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 9, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy("order_id")
)
