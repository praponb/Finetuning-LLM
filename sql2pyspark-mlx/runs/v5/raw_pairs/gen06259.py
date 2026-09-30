from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Grocery")
    .select("launch_date", F.lit("product").alias("origin"))
    .unionByName(
        df_inventory
        .filter(F.month("last_restock").isin([1, 7, 11]))
        .select(
        F.col("last_restock").alias("launch_date"),
        F.lit("inventory").alias("origin"),
    )
    )
)
