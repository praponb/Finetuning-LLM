from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())

ranked = (
    df_inventory
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select("id", "last_restock", "rank_in_group")
    .orderBy(F.col("id").desc_nulls_first())
)
