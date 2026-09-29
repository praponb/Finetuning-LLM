from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Singapore", "Bangkok", "New York"])
    .agg(F.avg("rating").alias("total"))
    .select(
        "country",
        F.col("Singapore").alias("singapore"),
        F.col("Bangkok").alias("bangkok"),
        F.col("New York").alias("new_york"),
    )
)
