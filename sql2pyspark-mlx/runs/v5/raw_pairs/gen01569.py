from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("country").isin(["JP", "SG"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(F.sum(F.when(F.col("country") == "TH", 1).otherwise(0)).alias("th_count"))
)
