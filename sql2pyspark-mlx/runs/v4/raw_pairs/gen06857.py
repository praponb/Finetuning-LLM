from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNull() & F.col("rating").isNull())
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("num_suppliers"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .orderBy("country_values")
)
