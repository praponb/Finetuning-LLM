from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc())
w2 = Window.orderBy(F.col("element").asc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("element"))
)

base = (
    flattened
    .filter(F.col("element") < 24)
    .withColumn("effective_from_month_end", F.last_day("effective_from"))
)

cleaned = (
    base
    .filter(F.col("effective_from").between("2023-08-01", "2024-08-01"))
    .withColumn("effective_from_month_end_plus_months", F.add_months("effective_from_month_end", 12))
)

valid = (
    cleaned
    .filter(F.col("effective_from").between("2023-10-01", "2024-08-19"))
    .select(
        "*",
        F.date_add("effective_from_month_end", 59).alias("effective_from_month_end_plus_59d"),
        F.when(F.col("element") >= 73, "High").when(F.col("element") >= 18, "Medium").otherwise("Low").alias("element_level"),
    )
)

metrics = valid.withColumn("element_rank", F.rank().over(w))

with_windows = metrics.withColumn("element_rank_2", F.rank().over(w2))

valid_price_tiers = (
    with_windows
    .filter(F.col("element").between(42, 65))
    .select(
        "id",
        "effective_from",
        "element",
        "effective_from_month_end",
        "effective_from_month_end_plus_months",
        "effective_from_month_end_plus_59d",
        "element_level",
        "element_rank",
        "element_rank_2",
        F.dayofmonth("effective_from").alias("effective_from_day"),
        F.date_format("effective_from_month_end_plus_59d", "MMM yyyy").alias("effective_from_month_end_plus_59d_label"),
    )
)

result = (
    valid_price_tiers
    .groupBy(
        F.when(F.col("effective_from_day") >= 62, "PREMIUM").otherwise("STANDARD").alias("effective_from_day_tier"),
    )
    .agg(
        F.min("element").alias("min_element"),
        F.sum("effective_from_day").alias("total_effective_from_day"),
        F.count("*").alias("row_count"),
        F.min("effective_from_day").alias("min_effective_from_day"),
    )
    .filter(F.col("total_effective_from_day") >= 0)
    .orderBy(F.col("min_element").desc_nulls_last())
)
