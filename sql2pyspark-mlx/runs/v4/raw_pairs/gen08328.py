from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(F.col("country").isin(["US", "TH"]))
    .withColumn("supplier_name_normalized", F.upper(F.trim("supplier_name")))
)

country_summary = (
    recent_suppliers
    .groupBy("country")
    .agg(
        F.count("*").alias("supplier_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
)

result = country_summary.select("country", "supplier_count")
