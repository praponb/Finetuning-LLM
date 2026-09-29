from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["TH", "SG"]) & F.col("supplier_name").like("%Tokyo%") & F.col("city").isin(["Tokyo", "New York", "Singapore"]))
    .groupBy(
        F.when(F.col("rating") >= 3.4, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.min("rating").alias("min_rating"),
        F.max("rating").alias("max_rating"),
    )
    .orderBy("max_rating")
)
