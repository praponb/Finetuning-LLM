from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "entry"))
    .select(
        "id",
        "customer_id",
        "created_at",
        (F.col("pos") + 1).alias("position"),
        F.col("entry.product_id"),
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

result = (
    lines
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .withColumn("product_id_tier", F.when(F.col("product_id") >= 11, "PREMIUM").otherwise("STANDARD"))
    .withColumn("position_tier", F.when(F.col("position") >= 3, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["activity_day", "product_id_tier", "position_tier"], ["activity_day", "product_id_tier"], ["activity_day"], []],
        "activity_day",
        "product_id_tier",
        "position_tier",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count_if(F.col("promo") == "SPRING10").alias("spring10_events"),
        F.max("created_at").alias("ended_at"),
        F.grouping("activity_day").alias("grouping_id"),
    )
)
