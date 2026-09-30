from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("rating") <= 3.8))
    .withColumn("rating_level", F.when(F.col("rating") >= 4.6, "High").when(F.col("rating") >= 1.6, "Medium").otherwise("Low"))
)

result = (
    base
    .select("id")
    .exceptAll(
        df_suppliers
        .filter(F.col("city").isin(["London", "Berlin", "Tokyo"]))
        .select("id")
    )
)
