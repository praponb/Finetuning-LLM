from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("supplier_name_country", F.concat("supplier_name", F.lit(", "), "country"))
)

result = (
    cleaned
    .groupBy("country")
    .pivot("city", ["New York", "Berlin", "London"])
    .agg(F.sum("rating"))
    .select(
        "country",
        F.col("New York").alias("new_york"),
        F.col("Berlin").alias("berlin"),
        F.col("London").alias("london"),
    )
)
