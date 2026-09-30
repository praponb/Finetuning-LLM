from pyspark.sql import functions as F

valid_stores = df_stores.filter((F.col("city") == "Bangkok") & (F.col("city") != "Berlin"))

stores_stats = (
    valid_stores
    .groupBy(
        F.when(F.col("region").isin(["South", "East"]), "priority").otherwise("other").alias("region_group"),
    )
    .agg(
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
)

result = stores_stats
