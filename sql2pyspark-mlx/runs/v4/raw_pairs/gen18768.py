from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("element"))
)

valid = (
    lines
    .filter((F.col("effective_from") > F.lit("2024-02-06").cast("date")) & (F.col("element") <= 59))
    .select(
        "*",
        F.when(F.col("element") >= 65, "High").when(F.col("element") >= 32, "Medium").otherwise("Low").alias("element_tier"),
    )
)

base = (
    valid
    .filter(F.col("element") >= 74)
    .withColumn("element_band", F.when(F.col("element") >= 69, "High").when(F.col("element") >= 20, "Medium").otherwise("Low"))
)

stats = (
    base
    .groupBy(
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
    )
    .agg(
        F.sum("element").alias("total_element"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("total_element") >= 0)
)

result = (
    stats
    .select("quarter_start", "year_start")
    .orderBy(F.col("year_start").desc())
)
