from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "Singapore") & F.upper("supplier_name").like("%ACI%") & F.col("country").isin(["US", "JP", "DE"]))
    .groupBy(
        F.when(F.col("city").isin(["Singapore", "Bangkok"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(
        F.count("*").alias("supplier_count"),
        F.min("rating").alias("min_rating"),
    )
    .orderBy("city_group", F.col("min_rating").desc())
)
