from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

with_last = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.last_restock"))
)

result = (
    with_last
    .groupBy(
        F.date_trunc("WEEK", "last_restock").alias("week_start"),
        F.when(F.col("stock_qty") >= 363, "PREMIUM").otherwise("STANDARD").alias("stock_qty_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and_2"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
    .orderBy("stock_qty_bit_and")
)
