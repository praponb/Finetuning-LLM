from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("supplier_name").like("Eur%"))
    .withColumn("country_lower", F.lower("country"))
)

cleaned_suppliers = (
    cleaned
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.length("country").alias("country_length"),
        F.concat("country", F.lit(", "), "supplier_name").alias("country_supplier_name"),
    )
)

kept = (
    cleaned_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date").between("2023-07-26", "2024-07-25")), "left_anti")
)

result = (
    kept
    .groupBy("country")
    .agg(
        F.stddev_samp("rating").alias("rating_stddev"),
        F.max(F.when(F.col("country") == "JP", F.col("country_length"))).alias("jp_max_country_length"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .orderBy(F.col("rating_stddev").desc())
)
