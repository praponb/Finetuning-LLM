from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(F.col("city").isin(["Bangkok", "Berlin", "London"]))
    .withColumn("is_sg", F.when(F.col("country") == "SG", 1).otherwise(0))
)

summary = (
    filtered_suppliers
    .groupBy(
        F.when(F.col("is_sg") >= 75, "high").when(F.col("is_sg") >= 38, "mid").otherwise("low").alias("is_sg_bucket"),
    )
    .agg(F.sum("is_sg").alias("total_is_sg"))
)

result = (
    summary
    .filter(F.col("total_is_sg") > 270)
    .select("is_sg_bucket", "total_is_sg")
    .orderBy("total_is_sg")
)
