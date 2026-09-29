from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location")
w2 = Window.partitionBy("dept_name")
w_part = Window.partitionBy("location")
w4 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_last())
w_grp = Window.orderBy(F.col("budget_adjusted").asc_nulls_last())
w6 = Window.orderBy(F.col("dept_name_max_budget_adjusted").desc_nulls_last())
w7 = Window.partitionBy("dept_name").orderBy(F.col("location_min_dept_name_max_budget_adjusted").desc_nulls_first())

cleaned = (
    df_departments
    .filter(F.col("budget") <= 2631000)
    .withColumn("budget_adjusted", F.round((F.col("budget") * 0.9), 2))
    .withColumn("clean_dept_name", F.trim("dept_name"))
)

matched_keys = (
    cleaned
    .select("location", "id")
    .intersect(
        df_departments
        .filter(F.col("dept_name") != "HR")
        .select("location", "id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.level") != "L5"), "left_anti")
)

scored = (
    kept
    .filter(F.col("location").isin(["Tokyo", "Chiang Mai"]))
    .select(
        "*",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.max("budget_adjusted").over(w2).alias("dept_name_max_budget_adjusted"),
        F.max("budget").over(w_part).alias("location_max_budget"),
    )
)

scored_departments = (
    scored
    .withColumn("location_min_dept_name_max_budget_adjusted", F.min("dept_name_max_budget_adjusted").over(w_part))
    .withColumn("budget_share_2", F.round((F.col("budget") / F.sum("budget").over(w_part)), 4))
    .withColumn("budget_dense_rank", F.dense_rank().over(w4))
)

metrics = (
    scored_departments
    .select(
        "id",
        "dept_name",
        "budget_adjusted",
        "budget_share",
        "location_max_budget",
        "location_min_dept_name_max_budget_adjusted",
        F.max("budget_share").over(w).alias("dept_name_max_budget_share"),
        F.rank().over(w_grp).alias("budget_adjusted_rank"),
        F.count("budget_adjusted").over(w_part).alias("location_count_budget_adjusted"),
        F.cume_dist().over(w6).alias("dept_name_max_budget_adjusted_cume_dist"),
        F.rank().over(w7).alias("location_min_dept_name_max_budget_adjusted_rank"),
    )
)

result = (
    metrics
    .withColumn("location_min_dept_name_max_budget_adjusted_rank_tier", F.when(F.col("location_min_dept_name_max_budget_adjusted_rank") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["location_min_dept_name_max_budget_adjusted_rank_tier", "dept_name"], ["location_min_dept_name_max_budget_adjusted_rank_tier"], []],
        "location_min_dept_name_max_budget_adjusted_rank_tier",
        "dept_name",
    )
    .agg(
        F.round(F.avg("dept_name_max_budget_share"), 2).alias("avg_dept_name_max_budget_share"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.min("location_min_dept_name_max_budget_adjusted").alias("min_location_min_dept_name_max_budget_adjusted"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("min_location_min_dept_name_max_budget_adjusted") > 0)
)
