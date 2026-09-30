from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first())
w_ordered = Window.orderBy(F.col("budget").desc())
w_recent = Window.partitionBy("dept_name", "location")
w5 = Window.partitionBy("dept_name")
w6 = Window.partitionBy("dept_name").orderBy(F.col("budget_rank").asc_nulls_first())
w7 = Window.partitionBy("location")
w8 = Window.orderBy(F.col("budget_rank").asc_nulls_first())

filtered = (
    df_departments
    .filter(F.col("location") != "Bangkok")
    .select(
        "*",
        F.when(F.col("location").isin(["Chiang Mai", "Tokyo"]), "Tier 1").otherwise("Other").alias("location_group"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    ranked
    .select("dept_name", "location")
    .exceptAll(
        df_departments
        .filter(F.col("location").isin(["Tokyo", "Chiang Mai", "New York"]))
        .select("dept_name", "location")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.percent_rank().over(w2).alias("budget_percent_rank"),
        F.rank().over(w_ordered).alias("budget_rank"),
        F.count("budget").over(w_recent).alias("dept_name_count_budget"),
        F.count("budget").over(w5).alias("dept_name_count_budget_2"),
    )
)

enriched_departments = (
    enriched
    .withColumn("budget_rank_dense_rank", F.dense_rank().over(w6))
    .withColumn("location_max_budget_percent_rank", F.max("budget_percent_rank").over(w7))
    .withColumn("dept_name_count_budget_share", F.round((F.col("dept_name_count_budget") / F.sum("dept_name_count_budget").over(w5)), 4))
    .withColumn("budget_rank_cume_dist", F.cume_dist().over(w8))
)

result = (
    enriched_departments
    .select(
        "id",
        "dept_name",
        "budget",
        "location_group",
        "budget_percent_rank",
        "budget_rank",
        "dept_name_count_budget",
        "dept_name_count_budget_2",
        "budget_rank_dense_rank",
        F.when(F.col("dept_name").rlike("^(Sales|HR)$"), "PRIORITY").when(F.col("dept_name").rlike("^(Engineering|Marketing)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("dept_name_zone"),
    )
    .orderBy(F.col("dept_name_zone").desc_nulls_first(), "budget")
)
