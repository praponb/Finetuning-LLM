from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-02-12").cast("date"))
    .select(
        "*",
        F.round((F.col("quantity") * 1.1), 2).alias("quantity_adjusted"),
        F.weekofyear("last_restock").alias("last_restock_weekofyear"),
    )
)

result = (
    base
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.brand"), F.col("p.launch_date"))
)
