from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(~((F.upper(F.trim("supplier_name")) == "PACIFIC WHOLESALE") | (F.col("city") == "Bangkok")))
    .withColumn("supplier_name_city_key", F.concat_ws(",", "supplier_name", "city"))
)

suppliers_stats = (
    src
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.countDistinct("rating").alias("unique_countrys"))
)

result = (
    suppliers_stats
    .filter(F.col("unique_countrys") >= 6)
    .select("rating_bucket", "unique_countrys")
    .orderBy("rating_bucket", F.col("unique_countrys").desc())
    .limit(1)
)
