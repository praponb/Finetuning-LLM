from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-11-15").cast("date"))
    .select(
        "*",
        F.when(F.col("quantity") >= 257, "High").when(F.col("quantity") >= 116, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.brand"), F.col("d.product_name"), F.col("d.cost"))
)
