from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("stock_qty", "id")
w2 = Window.partitionBy("supplier_id").orderBy("cost", "id")

enriched = (
    df_products
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(3).over(w2).alias("cost_bucket"),
    )
)

result = (
    enriched
    .rollup(
        F.when(F.col("cost") >= 750, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
        "category",
        F.when(F.col("stock_qty") >= 364, "HIGH").otherwise("LOW").alias("stock_qty_tier"),
    )
    .agg(
        F.percentile_approx("stock_qty", 0.5).alias("stock_qty_p50"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
        F.count_if(F.col("category") == "Sports").alias("sports_events"),
    )
    .orderBy(F.col("cost_tier").desc_nulls_last())
)
