from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() & F.col("city").isin(["New York", "London"]))
    .groupBy(
        "city",
        F.when(F.col("city").isin(["Chiang Mai", "Singapore"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.avg("rating").alias("avg_rating"),
        F.sum("rating").alias("total_rating"),
    )
)
