from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w_hist_2 = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity_filled").desc(), "id")
w4 = Window.partitionBy("store_id").orderBy("quantity_filled", "id")

base = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2024-04-05").cast("date")) & F.month("last_restock").isin([5, 8, 10]))
    .select(
        "*",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.when(F.col("quantity") >= 207, "High").when(F.col("quantity") >= 48, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

with_windows = (
    base
    .withColumn("event_seq", F.row_number().over(w_hist))
    .withColumn("event_seq_2", F.row_number().over(w_hist_2))
    .withColumn("quantity_filled_bucket", F.ntile(3).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

with_dims = (
    with_windows
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.stock_qty"), F.col("d.price"), F.col("d.category"))
)

stats = (
    with_dims
    .groupBy("seq_no")
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
        F.count_if(F.col("category") == "Books").alias("books_events"),
    )
)

result = (
    stats
    .select("seq_no", "fashion_count")
    .orderBy(F.col("seq_no").desc())
)
