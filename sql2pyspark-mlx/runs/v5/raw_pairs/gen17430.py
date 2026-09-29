from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")

filtered = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-07-09"))) & F.col("store_id").isin([3, 4]))
    .select(
        "id",
        "store_id",
        "quantity",
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)
