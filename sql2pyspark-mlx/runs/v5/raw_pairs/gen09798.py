from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc())
w2 = Window.orderBy("salary")
w3 = Window.partitionBy("dept_id")
w4 = Window.partitionBy("dept_id", "manager_id").orderBy("salary")
w5 = Window.partitionBy("location")
w6 = Window.orderBy(F.col("salary_cume_dist").desc_nulls_first())
w7 = Window.partitionBy("location").orderBy("salary")
w8 = Window.partitionBy("location").orderBy(F.col("salary_percent_rank").desc())
w_hist = Window.orderBy(F.col("salary_cume_dist_2").desc_nulls_first())

remaining_keys = (
    df_employees
    .select("level", "manager_id")
    .subtract(
        df_employees
        .filter(F.col("is_remote"))
        .select("level", "manager_id")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.dept_name"), F.col("d.location"))
)

enriched = (
    with_dims
    .withColumn("salary_cume_dist", F.cume_dist().over(w))
    .withColumn("salary_cume_dist_2", F.cume_dist().over(w2))
)

with_windows = (
    enriched
    .select(
        "id",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "dept_name",
        "location",
        "salary_cume_dist_2",
        F.avg("salary_cume_dist").over(w3).alias("dept_id_avg_salary_cume_dist"),
        F.percent_rank().over(w4).alias("salary_percent_rank"),
        F.max("salary_cume_dist_2").over(w5).alias("location_max_salary_cume_dist_2"),
        F.dense_rank().over(w6).alias("salary_cume_dist_dense_rank"),
    )
)

metrics = (
    with_windows
    .filter(F.col("dept_name") == "Legal")
    .withColumn("salary_percent_rank_2", F.percent_rank().over(w7))
    .withColumn("salary_percent_rank_rank", F.rank().over(w8))
    .withColumn("salary_cume_dist_2_percent_rank", F.percent_rank().over(w_hist))
)

result = (
    metrics
    .select(
        "dept_name",
        "salary_cume_dist_2",
        "salary_cume_dist_2_percent_rank",
        "salary_percent_rank",
    )
    .unpivot(
        ids=["dept_name"],
        values=["salary_cume_dist_2", "salary_cume_dist_2_percent_rank", "salary_percent_rank"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
