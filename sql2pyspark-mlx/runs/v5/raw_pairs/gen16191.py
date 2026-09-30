from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "device",
        F.when(F.col("country").isin(["SG", "JP"]), "Group A").otherwise("Other").alias("country_group"),
        F.when(F.col("country") == "TH", 1).otherwise(0).alias("is_th"),
    )
)
