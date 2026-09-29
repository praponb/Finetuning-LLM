from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("num_currencies").desc())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("element"))
)

result = (
    flattened
    .groupBy(
        "currency",
        F.when(F.col("element") >= 75, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
    )
    .agg(
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles_2"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
    .withColumn("num_currencies_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("currency").asc_nulls_first())
)
