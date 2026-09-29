from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("plan_avg_element_3").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("plan_avg_element_3").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("plan_avg_element_2").desc())
w_ordered = Window.orderBy(F.col("element").desc_nulls_last())
w6 = Window.orderBy(F.col("plan_avg_element_3_cume_dist").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("element"),
    )
)

scored = (
    exploded
    .filter(F.col("effective_from").between("2025-05-01", "2025-09-01"))
    .select(
        "*",
        F.avg("element").over(w_grp).alias("plan_avg_element"),
        F.avg("element").over(w_grp).alias("plan_avg_element_2"),
        F.avg("element").over(w_grp).alias("plan_avg_element_3"),
    )
)

scored_price_tiers = (
    scored
    .select(
        "*",
        F.avg("plan_avg_element_2").over(w_grp).alias("plan_avg_plan_avg_element_2"),
        F.count("element").over(w_grp).alias("plan_count_element"),
        F.dense_rank().over(w2).alias("plan_avg_element_3_dense_rank"),
        F.cume_dist().over(w3).alias("plan_avg_element_3_cume_dist"),
        F.percent_rank().over(w4).alias("plan_avg_element_2_percent_rank"),
    )
)

valid = (
    scored_price_tiers
    .filter(F.col("plan").isin(["starter", "pro", "enterprise"]))
    .select(
        "id",
        "element",
        "plan_avg_element_2",
        "plan_avg_plan_avg_element_2",
        "plan_count_element",
        "plan_avg_element_3_dense_rank",
        "plan_avg_element_3_cume_dist",
        F.when(F.col("plan_count_element") >= 13, "High").when(F.col("plan_count_element") >= 5, "Medium").otherwise("Low").alias("plan_count_element_level"),
        F.round((F.col("element") * 1.15), 2).alias("element_adjusted"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.cume_dist().over(w_ordered).alias("element_cume_dist"),
        F.rank().over(w6).alias("plan_avg_element_3_cume_dist_rank"),
    )
)

stats = (
    metrics
    .groupBy(
        F.when(F.col("element_cume_dist") >= 0.8, "LARGE").otherwise("SMALL").alias("element_cume_dist_tier"),
        F.when(F.col("plan_count_element") >= 14, "LARGE").otherwise("SMALL").alias("plan_count_element_tier"),
    )
    .agg(
        F.percentile_approx("plan_avg_element_3_cume_dist", 0.95).alias("plan_avg_element_3_cume_dist_p95"),
        F.sum("plan_count_element").alias("total_plan_count_element"),
        F.count("*").alias("row_count"),
    )
)

result = (
    stats
    .select(
        "element_cume_dist_tier",
        "plan_count_element_tier",
        "plan_avg_element_3_cume_dist_p95",
        "total_plan_count_element",
        "row_count",
    )
)
