from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "New York", 1).otherwise(0)).alias("new_york_count"),
    )
    .filter(F.col("new_york_count") > 42)
)
