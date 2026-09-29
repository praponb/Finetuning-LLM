from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("element"))
)

result = (
    lines
    .withColumn("year_start", F.date_trunc("YEAR", "effective_from"))
    .withColumn("element_tier", F.when(F.col("element") >= 77, "PREMIUM").otherwise("STANDARD"))
    .cube("year_start", "element_tier")
    .agg(
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.sum(F.when(F.col("currency") == "USD", F.col("element")).otherwise(0)).alias("usd_element"),
        F.round(F.avg("element"), 2).alias("avg_element_2"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("usd_element") > 0)
    .orderBy(F.col("avg_element").asc_nulls_first())
)
