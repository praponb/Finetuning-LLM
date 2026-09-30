from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["SG", "JP"])
    .agg(F.avg("rating").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "city",
        F.col("SG_amount").alias("sg_amount"),
        F.col("SG_item_count").alias("sg_item_count"),
        F.col("JP_amount").alias("jp_amount"),
        F.col("JP_item_count").alias("jp_item_count"),
    )
)
