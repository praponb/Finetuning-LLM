from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .withColumn("opened_date_plus_43d", F.date_add("opened_date", 43))
)

region_summary = (
    valid_stores
    .groupBy("region")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
)

result = region_summary.orderBy(F.col("region_values").desc())
