from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["UK", "JP", "TH"])
    .agg(F.max("rating").alias("amount"))
    .select(
        "city",
        F.col("UK").alias("uk"),
        F.col("JP").alias("jp"),
        F.col("TH").alias("th"),
    )
)
