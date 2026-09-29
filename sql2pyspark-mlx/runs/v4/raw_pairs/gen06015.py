from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0)).alias("grocery_count"),
    )
    .filter(F.col("grocery_count") < 51)
    .select("launch_year", "cost_range", "grocery_count")
)
