from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "is_active",
        F.when(F.col("country") == "TH", 1).otherwise(0).alias("is_th"),
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
    .orderBy("is_th")
)
