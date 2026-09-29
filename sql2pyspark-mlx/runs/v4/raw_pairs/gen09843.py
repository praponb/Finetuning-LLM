from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first(), "id")
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_first(), "id")

filtered = (
    df_departments
    .filter(F.col("budget").between(2278000, 3555000))
    .select(
        "*",
        F.when(F.col("dept_name").isin(["Legal", "Marketing"]), "Group A").otherwise("Other").alias("dept_name_group"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (~F.col("c.is_remote")), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

valid = (
    ranked
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "dept_name_group",
        "rank_in_group",
        F.greatest("budget", "rank_in_group").alias("greatest_budget_rank_in_group"),
        F.substring("location", 1, 2).alias("location_prefix"),
    )
)

result = (
    valid
    .withColumn("greatest_budget_rank_in_group_tier", F.when(F.col("greatest_budget_rank_in_group") >= 83, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["dept_name", "greatest_budget_rank_in_group_tier"], ["dept_name"], []],
        "dept_name",
        "greatest_budget_rank_in_group_tier",
    )
    .agg(
        F.sum(F.when(F.col("location") == "London", F.col("greatest_budget_rank_in_group"))).alias("london_total_greatest_budget_rank_in_group"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .orderBy(F.col("dept_name").desc())
)
