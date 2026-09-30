from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("currency").orderBy(F.col("val_percent_rank").desc_nulls_last())

exploded = df_price_tiers.select("id", "currency", F.explode_outer("tier_limits").alias("val"))

valid = (
    exploded
    .filter(F.col("val").isNotNull())
    .withColumn("currency_group", F.when(F.col("currency").isin(["USD", "EUR"]), "Core").otherwise("Other"))
)

enriched = (
    valid
    .select(
        "id",
        "currency",
        F.rank().over(w).alias("val_rank"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.count("val_percent_rank").over(w3).alias("currency_count_val_percent_rank"),
        F.rank().over(w4).alias("val_percent_rank_rank"),
        F.max("val_rank").over(w3).alias("currency_max_val_rank"),
    )
)

result = (
    metrics
    .groupBy("currency")
    .agg(
        F.sum("val_rank").alias("total_val_rank"),
        F.sum(F.when(F.col("currency") == "USD", F.col("val_percent_rank_rank")).otherwise(0)).alias("usd_val_percent_rank_rank"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.count("*").alias("row_count_2"),
    )
    .filter(F.col("usd_events") < 1000)
)
