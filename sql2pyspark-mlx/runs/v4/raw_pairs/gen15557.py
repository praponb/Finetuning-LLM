from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy("greatest_element_element_rounded")

exploded = df_price_tiers.select("id", "currency", F.explode("discounts").alias("element"))

valid = (
    exploded
    .filter(F.col("currency") == "EUR")
    .select("*", F.round("element", 1).alias("element_rounded"))
)

base = (
    valid
    .filter(F.col("element_rounded") <= 65)
    .select(
        "*",
        F.greatest("element", "element_rounded").alias("greatest_element_element_rounded"),
    )
)

remaining_keys = (
    base
    .select("id")
    .exceptAll(
        df_price_tiers
        .filter(F.col("effective_from") <= "2024-09-01")
        .select("id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("currency_min_element", F.min("element").over(w_recent))
    .withColumn("greatest_element_element_rounded_rank", F.rank().over(w2))
)

result = (
    scored
    .select(
        "id",
        "currency",
        "element",
        "element_rounded",
        "greatest_element_element_rounded",
        "currency_min_element",
        "greatest_element_element_rounded_rank",
        F.when(F.col("currency").rlike("^(USD|EUR)$"), "PRIORITY").when(F.col("currency").rlike("^(THB)$"), "CORE").otherwise("GROUP_B").alias("currency_zone"),
    )
    .orderBy(F.col("element").desc())
)
