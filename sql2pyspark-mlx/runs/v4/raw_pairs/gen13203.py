from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element_tier").orderBy(F.col("min_element").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("element") >= 76, "HIGH").otherwise("LOW").alias("element_tier"),
        "currency",
    )
    .agg(
        F.max(F.when(F.col("plan") == "enterprise", F.col("element"))).alias("enterprise_max_element"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("element_per_enterprise"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.min("element").alias("min_element"),
    )
    .withColumn("min_element_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("enterprise_max_element").desc_nulls_last())
)
