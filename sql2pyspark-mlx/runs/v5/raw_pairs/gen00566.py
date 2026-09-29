from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element_minus_element_whole").asc_nulls_first())
w2 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

filtered = (
    exploded
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-04-03"))) & F.col("effective_from").between("2024-03-19", "2024-05-10"))
    .withColumn("element_whole", F.ceil("element"))
    .withColumn("element_int", F.col("element").cast("int"))
)

valid = (
    filtered
    .filter((F.col("effective_from") > F.lit("2024-03-25").cast("date")) & (F.year("effective_from") >= 2023))
    .select(
        "currency",
        "element_int",
        (F.col("element") - F.col("element_whole")).alias("element_minus_element_whole"),
        F.upper("currency").alias("currency_upper"),
    )
)

with_windows = (
    valid
    .filter(~F.col("currency").isin(["THB", "USD"]))
    .withColumn("element_minus_element_whole_rank", F.rank().over(w))
    .withColumn("currency_sum_element_minus_element_whole", F.sum("element_minus_element_whole").over(w2))
)

filtered_price_tiers = (
    with_windows
    .filter(F.col("element_minus_element_whole_rank") <= 6)
    .select(
        "element_minus_element_whole",
        "currency_upper",
        "element_minus_element_whole_rank",
        "currency_sum_element_minus_element_whole",
        (F.col("element_int") * F.col("element_minus_element_whole_rank")).alias("element_int_times_element_minus_element_whole_rank"),
        (F.col("currency_sum_element_minus_element_whole") + F.col("element_minus_element_whole_rank")).alias("currency_sum_element_minus_element_whole_plus_element_minus_element_whole_rank"),
    )
)

result = (
    filtered_price_tiers
    .select(
        "element_minus_element_whole",
        "element_minus_element_whole_rank",
        "currency_sum_element_minus_element_whole",
        "element_int_times_element_minus_element_whole_rank",
        "currency_sum_element_minus_element_whole_plus_element_minus_element_whole_rank",
    )
    .orderBy(
        F.col("currency_sum_element_minus_element_whole_plus_element_minus_element_whole_rank").desc(),
    )
)
