from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "city",
        F.when(F.col("city").isin(["Berlin", "Tokyo"]), "priority").otherwise("other").alias("city_group"),
    )
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
)
