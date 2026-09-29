from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.concat_ws(",", "supplier_name", "country").alias("supplier_name_country_key"),
    )
)

result = (
    base
    .groupBy("city")
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
    )
)
