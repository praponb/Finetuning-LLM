from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["JP", "UK", "DE"]) & F.col("city").isin(["Berlin", "New York", "Tokyo"]))
    .select(
        "supplier_name",
        "city",
        F.coalesce("city", F.lit("unassigned")).alias("city_filled"),
        F.concat("city", F.lit("/"), "country").alias("city_country"),
    )
    .orderBy("supplier_name", F.col("city_filled").desc())
)
