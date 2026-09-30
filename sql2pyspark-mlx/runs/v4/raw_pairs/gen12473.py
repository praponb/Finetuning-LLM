from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy(F.col("stock_qty").desc(), "id")

base = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2024-02-08").cast("date")) & F.col("brand").isNotNull())
    .select(
        "*",
        (F.col("price") + F.col("stock_qty")).alias("price_plus_stock_qty"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
