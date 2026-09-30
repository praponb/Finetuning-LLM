from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc())

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-04-03").cast("date"))
    .withColumn("quantity_whole", F.floor("quantity"))
)

result = (
    valid
    .select(
        "*",
        F.last("quantity", ignorenulls=True).over(w).alias("last_known_quantity"),
        F.percent_rank().over(w2).alias("quantity_percent_rank"),
    )
)
