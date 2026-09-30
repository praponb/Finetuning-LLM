from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")

deduped = (
    df_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("price") >= 1260, "LARGE").otherwise("SMALL").alias("price_tier"),
    )
    .agg(
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Stark", 1)), F.lit(0)), 2).alias("stock_qty_per_stark"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.countDistinct("brand").alias("distinct_brands"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_rows"),
        F.max("price").alias("max_price"),
    )
    .orderBy(F.col("max_price").desc())
)
