from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc())
w2 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_last())
w_grp = Window.orderBy(F.col("element_cume_dist_2_scaled").desc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("element"))
)

enriched = (
    exploded
    .select(
        "id",
        "currency",
        "element",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.cume_dist().over(w2).alias("element_cume_dist_2"),
    )
)

valid = (
    enriched
    .filter(F.col("currency").isin(["THB", "EUR"]))
    .select(
        "id",
        "currency",
        "element",
        "element_cume_dist",
        "element_cume_dist_2",
        (F.col("element") * F.col("element_cume_dist")).alias("element_times_element_cume_dist"),
    )
)

base = (
    valid
    .filter(F.col("element").isNotNull())
    .select(
        "id",
        "element_cume_dist",
        "element_cume_dist_2",
        "element_times_element_cume_dist",
        F.round((F.col("element_cume_dist_2") * 1.07), 2).alias("element_cume_dist_2_scaled"),
        F.col("element_times_element_cume_dist").cast("int").alias("element_times_element_cume_dist_int"),
    )
)

metrics = (
    base
    .select(
        "element_times_element_cume_dist",
        "element_cume_dist_2_scaled",
        "element_times_element_cume_dist_int",
        F.percent_rank().over(w_grp).alias("element_cume_dist_2_scaled_percent_rank"),
    )
)

result = (
    metrics
    .select("element_cume_dist_2_scaled", "element_times_element_cume_dist_int")
    .orderBy(F.col("element_cume_dist_2_scaled").desc_nulls_first())
)
