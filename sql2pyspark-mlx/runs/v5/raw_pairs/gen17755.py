from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val_filled").desc())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())
w4 = Window.partitionBy("plan").orderBy("val_filled")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

valid = (
    exploded
    .filter((F.col("effective_from") >= F.lit("2024-06-05").cast("date")) & (F.col("val") <= 80))
    .withColumn("val_filled", F.coalesce("val", F.lit(0)))
    .withColumn("clean_currency", F.trim("currency"))
)

enriched = (
    valid
    .filter(F.col("val_filled").isNull())
    .select(
        "plan",
        "effective_from",
        "val",
        "val_filled",
        "clean_currency",
        F.dense_rank().over(w).alias("val_filled_dense_rank"),
        F.count("val").over(w2).alias("currency_count_val"),
        F.count("val_filled").over(w2).alias("currency_count_val_filled"),
        F.rank().over(w3).alias("val_rank"),
    )
)

scored = enriched.select("*", F.rank().over(w4).alias("val_filled_rank"))

result = (
    scored
    .groupBy(F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.percentile_approx("currency_count_val_filled", [0.25, 0.5, 0.75]).alias("currency_count_val_filled_quartiles"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
    )
    .filter(F.col("enterprise_count") > 1)
)
