from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~((F.length("supplier_name") > 12) & (F.length("supplier_name") < 13) & ~F.col("city").isin(["Bangkok", "New York"])))
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
    .orderBy(F.col("rating_bucket").desc(), F.col("rating_range").desc())
)
