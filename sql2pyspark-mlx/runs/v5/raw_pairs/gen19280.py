from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("days_since_created_at").asc_nulls_last())
w2 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select("id", "created_at", F.col("elem.price"), F.col("elem.promo"))
)

base = (
    exploded
    .filter((~F.col("promo").eqNullSafe(F.lit("SPRING10"))) & F.col("price").between(160, 240))
    .select(
        "*",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at_2"),
    )
)

enriched = base.select("*", F.dense_rank().over(w).alias("days_since_created_at_dense_rank"))

metrics = (
    enriched
    .withColumn("price_max_1h", F.max("price").over(w2))
    .withColumn("days_since_created_at_dense_rank_avg_15m", F.avg("days_since_created_at_dense_rank").over(w3))
)

result = (
    metrics
    .cube(
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        F.when(F.col("days_since_created_at_2") >= 84, "LARGE").otherwise("SMALL").alias("days_since_created_at_2_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("promo") == "VIP", F.col("days_since_created_at_dense_rank_avg_15m")).otherwise(0)).alias("vip_days_since_created_at_dense_rank_avg_15m"),
        F.count("*").alias("num_rows"),
    )
)
