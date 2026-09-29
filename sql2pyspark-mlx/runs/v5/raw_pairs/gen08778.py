from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-08-16").cast("date")) & (F.col("store_id") == 6))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.date_trunc("WEEK", "last_restock").alias("last_restock_period"),
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.store_name"), F.col("d.city"))
)
