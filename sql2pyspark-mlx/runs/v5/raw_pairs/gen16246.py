from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-04-11")))
    .select(
        "*",
        F.when(F.col("city").isin(["Bangkok", "Chiang Mai"]), "Priority").otherwise("Other").alias("city_group"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)
