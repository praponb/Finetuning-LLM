from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty_times_cost").desc_nulls_last(), "id")

valid = (
    df_products
    .filter(F.col("brand") == "Umbrella")
    .select(
        "*",
        (F.col("stock_qty") * F.col("cost")).alias("stock_qty_times_cost"),
        F.initcap(F.trim("brand")).alias("brand_title"),
    )
)

result = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
