from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc())
w2 = Window.partitionBy("currency")
w3 = Window.orderBy(F.col("element_int").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("element"))
)

scored = exploded.select("*", F.cume_dist().over(w).alias("element_cume_dist"))

cleaned = (
    scored
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-05-05"))) & (F.col("currency") == "EUR"))
    .select("*", F.round((F.col("element") * 1.1), 2).alias("element_scaled"))
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("effective_from") >= F.lit("2024-06-20").cast("date"))
    .select("*", F.col("element").cast("int").alias("element_int"))
)

scored_price_tiers = (
    cleaned_price_tiers
    .select(
        "*",
        F.sum("element_cume_dist").over(w2).alias("currency_sum_element_cume_dist"),
        F.max("element_int").over(w2).alias("currency_max_element_int"),
        F.cume_dist().over(w3).alias("element_int_cume_dist"),
        F.avg("element").over(w2).alias("currency_avg_element"),
    )
)

result = (
    scored_price_tiers
    .groupBy("currency")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("currency_avg_element"), 2).alias("avg_currency_avg_element"),
    )
    .filter(F.col("avg_currency_avg_element") >= 0)
)
