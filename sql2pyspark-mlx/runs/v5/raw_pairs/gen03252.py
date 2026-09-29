from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Siam%") & F.col("country").isin(["DE", "TH", "SG"]))
    .groupBy("country")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)
