from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_last())

cleaned = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-04-28").cast("date")) & F.col("product_id").isin([2, 5, 7, 9]))
    .withColumn("quantity_num", F.col("quantity").cast("string"))
)

result = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.sum("quantity").over(w2).alias("cumulative_quantity"),
        F.rank().over(w_running).alias("quantity_rank"),
    )
)
