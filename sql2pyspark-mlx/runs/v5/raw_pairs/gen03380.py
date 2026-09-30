from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("budget") >= 1842000) & F.col("location").isin(["Singapore", "Tokyo", "Berlin"])) | ~F.col("location").isin(["Bangkok", "Tokyo"]))
    .groupBy("location")
    .agg(
        F.count("budget").alias("budget_count"),
        F.sum(F.when(F.col("location") == "London", 1).otherwise(0)).alias("london_count"),
    )
    .orderBy(F.col("london_count").desc(), F.col("location").desc())
)
