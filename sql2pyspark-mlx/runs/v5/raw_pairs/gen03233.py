from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.length("supplier_name") < 9)
    .withColumn("city_filled", F.coalesce("city", F.lit("unassigned")))
)

country_summary = (
    src
    .groupBy("country")
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("supplier_count"),
    )
)

result = (
    country_summary
    .select("country", "supplier_count")
    .orderBy(F.col("country").desc())
)
