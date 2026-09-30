from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("element").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

cleaned = (
    exploded
    .filter(F.col("element").isNotNull())
    .select("id", "plan", "element", F.trim("plan").alias("clean_plan"))
)

filtered = (
    cleaned
    .filter(F.col("element").between(28, 67))
    .select("*", F.trim("plan").alias("clean_plan_2"))
)

enriched = (
    filtered
    .filter(F.col("element") <= 63)
    .select(
        "*",
        F.max("element").over(w_running).alias("plan_max_element"),
        F.cume_dist().over(w2).alias("element_cume_dist"),
        F.count("element").over(w_running).alias("plan_count_element"),
        F.cume_dist().over(w3).alias("element_cume_dist_2"),
    )
)

filtered_price_tiers = (
    enriched
    .filter(F.col("plan") != "pro")
    .select(
        "*",
        F.coalesce("element_cume_dist_2", F.lit(0)).alias("element_cume_dist_2_filled"),
        (F.col("plan_count_element") / F.nullif("plan_max_element", F.lit(0))).alias("plan_count_element_per_plan_max_element"),
    )
)

result = (
    filtered_price_tiers
    .select(
        "plan",
        F.col("element_cume_dist").alias("ELEMENT_CUME_DIST_VAL"),
        F.col("element").alias("ELEMENT_VAL"),
        F.col("plan_count_element_per_plan_max_element").alias("PLAN_COUNT_ELEMENT_PER_PLAN_MAX_ELEMENT"),
        F.col("element_cume_dist_2_filled").alias("ELEMENT_CUME_DIST_2_FILLED_VAL"),
    )
    .unpivot(
        ids=["plan"],
        values=["ELEMENT_CUME_DIST_VAL", "ELEMENT_VAL", "PLAN_COUNT_ELEMENT_PER_PLAN_MAX_ELEMENT", "ELEMENT_CUME_DIST_2_FILLED_VAL"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
