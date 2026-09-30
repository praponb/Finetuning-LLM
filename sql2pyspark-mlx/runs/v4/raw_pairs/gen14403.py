from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.month("launch_date").isin([1, 2, 5]) & (F.col("cost") >= 290))
    .agg(
        F.count("price").alias("price_count"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("cost")).otherwise(0)).alias("grocery_cost"),
    )
)
