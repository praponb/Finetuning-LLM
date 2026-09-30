from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.year("opened_date") >= 2023).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("i.quantity").isNotNull())
    .select(F.col("i.id"), F.col("i.quantity"))
    .orderBy("quantity")
)
