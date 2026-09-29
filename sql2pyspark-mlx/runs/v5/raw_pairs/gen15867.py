from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")

base = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 6]))
    .select(
        "id",
        "store_id",
        "quantity",
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
    )
)

result = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id", "quantity")
)
