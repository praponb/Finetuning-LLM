from pyspark.sql import functions as F

result = (
    df_departments
    .agg(
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .orderBy("chiang_mai_count")
)
