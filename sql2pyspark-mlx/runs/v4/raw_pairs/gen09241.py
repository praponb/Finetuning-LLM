from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")

valid = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4]))
    .select("*", F.add_months("last_restock", 12).alias("last_restock_plus_months"))
)

result = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
