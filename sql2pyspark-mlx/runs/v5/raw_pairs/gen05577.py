from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-08-21").cast("date")) & F.col("store_id").isin([1, 4]))
    .withColumn("quantity_band", F.when(F.col("quantity") >= 209, "High").when(F.col("quantity") >= 38, "Medium").otherwise("Low"))
)

result = (
    valid
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.quantity_band",
        F.col("p.store_name"),
        F.col("p.opened_date"),
    )
)
