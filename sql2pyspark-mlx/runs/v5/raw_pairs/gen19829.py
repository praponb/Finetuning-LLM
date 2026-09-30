from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select("id", F.col("line.promo"))
)

result = (
    lines
    .groupBy("promo")
    .agg(
        F.count_if(F.col("promo") == "VIP").alias("vip_events"),
        F.count_if(F.col("promo") == "SPRING10").alias("spring10_events"),
        F.count_if(F.col("promo") == "VIP").alias("vip_events_2"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("spring10_events") < 1000)
)
