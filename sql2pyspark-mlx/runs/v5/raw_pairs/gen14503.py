from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter((F.col("city") == "London") & (F.col("opened_date") < "2024-05-01"))
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM-dd"))
)

agg = (
    valid_stores
    .groupBy(
        "city",
        F.when(F.col("region").isin(["Central", "South"]), "tier_1").otherwise("other").alias("region_group"),
    )
    .agg(F.sort_array(F.collect_set("region")).alias("region_list"))
)

result = agg.select("city", "region_group")
