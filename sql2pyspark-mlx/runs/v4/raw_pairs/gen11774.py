from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(F.year("opened_date") == 2025)
    .withColumn("adjusted_sqft", F.round((F.col("sqft") * 100), 2))
)

summary = (
    filtered_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "region")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
)

result = (
    summary
    .select("opened_quarter", "region", "region_values")
    .orderBy("opened_quarter", F.col("region").desc())
)
