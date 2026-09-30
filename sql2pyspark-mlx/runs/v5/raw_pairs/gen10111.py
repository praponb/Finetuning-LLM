from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())
w4 = Window.partitionBy("manager_id")
w5 = Window.partitionBy("manager_id").orderBy(F.col("salary_avg_2").desc())
w_grp = Window.orderBy(F.col("salary_rank_plus_salary_avg_2_share_2").asc_nulls_first())
w_recent = Window.orderBy(F.col("salary_avg_2").desc())
w8 = Window.partitionBy("activity_month").orderBy(F.col("distinct_ids").desc())

metrics = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "hire_date",
        "is_remote",
        F.rank().over(w).alias("salary_rank"),
        F.avg("salary").over(w2).alias("salary_avg_2"),
        F.dense_rank().over(w3).alias("salary_dense_rank"),
    )
)

kept_keys = (
    metrics
    .select("manager_id")
    .subtract(
        df_employees
        .filter(F.col("manager_id").isin([15, 18, 22, 27]))
        .select("manager_id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("hire_date") > F.to_date(F.lit("2023-08-19")))
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
)

scored = (
    filtered
    .select(
        "id",
        "full_name",
        "manager_id",
        "hire_date",
        "salary_rank",
        "salary_avg_2",
        "salary_dense_rank",
        F.round((F.col("salary_avg_2") / F.sum("salary_avg_2").over(w4)), 4).alias("salary_avg_2_share"),
    )
)

scored_employees = (
    scored
    .filter(F.col("salary_dense_rank") < 6)
    .withColumn("salary_avg_2_share_2", F.round((F.col("salary_avg_2") / F.sum("salary_avg_2").over(w4)), 4))
    .withColumn("manager_id_count_salary_dense_rank", F.count("salary_dense_rank").over(w4))
    .withColumn("salary_avg_2_rank", F.rank().over(w5))
)

cleaned = (
    scored_employees
    .filter(F.col("salary_dense_rank") > 5)
    .select(
        "id",
        "full_name",
        "hire_date",
        "salary_rank",
        "salary_avg_2",
        "salary_dense_rank",
        "salary_avg_2_share_2",
        "manager_id_count_salary_dense_rank",
        "salary_avg_2_rank",
        F.last_day("hire_date").alias("hire_date_month_end"),
        (F.col("salary_rank") + F.col("salary_avg_2_share_2")).alias("salary_rank_plus_salary_avg_2_share_2"),
    )
)

scored_employees_2 = (
    cleaned
    .select(
        "id",
        "full_name",
        "hire_date",
        "salary_avg_2",
        "salary_dense_rank",
        "salary_avg_2_rank",
        F.dense_rank().over(w_grp).alias("salary_rank_plus_salary_avg_2_share_2_dense_rank"),
        F.rank().over(w_recent).alias("salary_avg_2_rank_2"),
    )
)

result = (
    scored_employees_2
    .groupBy(
        F.date_trunc("month", "hire_date").alias("activity_month"),
        F.when(F.col("salary_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("salary_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("salary_rank_plus_salary_avg_2_share_2_dense_rank", [0.25, 0.5, 0.75]).alias("salary_rank_plus_salary_avg_2_share_2_dense_rank_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("salary_avg_2_rank").alias("min_salary_avg_2_rank"),
    )
    .filter(F.col("distinct_ids") > 1)
    .withColumn("distinct_ids_rank", F.rank().over(w8))
)
