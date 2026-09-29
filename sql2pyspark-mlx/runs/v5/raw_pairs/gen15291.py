from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter((F.col("city") != "New York") & F.lower("supplier_name").like("%aci%"))
)

summary = (
    recent_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)

result = (
    summary
    .select("city", "rating_bucket")
    .orderBy(F.col("rating_bucket").desc(), "city")
    .limit(1)
)
