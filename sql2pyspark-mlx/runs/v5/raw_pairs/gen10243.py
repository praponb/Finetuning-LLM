from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city").eqNullSafe(F.lit("New York"))))
    .select(
        "*",
        F.concat("supplier_name", F.lit(" - "), "city").alias("supplier_name_city"),
    )
)

result = (
    valid
    .groupBy("country")
    .pivot("city", ["New York", "Chiang Mai"])
    .agg(F.sum("rating"))
    .select(
        "country",
        F.col("New York").alias("new_york"),
        F.col("Chiang Mai").alias("chiang_mai"),
    )
)
