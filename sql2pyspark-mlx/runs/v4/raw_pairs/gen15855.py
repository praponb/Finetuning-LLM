from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(F.upper(F.trim("supplier_name")) == "TOKYO TRADERS")
    .withColumn("city_lower", F.lower("city"))
)

suppliers_stats = (
    recent_suppliers
    .groupBy("country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.min("rating").alias("min_rating"),
    )
)

result = (
    suppliers_stats
    .select("country", "city_values", "min_rating")
    .orderBy("city_values")
)
