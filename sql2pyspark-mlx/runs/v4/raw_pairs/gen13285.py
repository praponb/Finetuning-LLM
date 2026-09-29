from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_first())
w2 = Window.partitionBy("location")

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("budget") >= 4013000))
    .withColumn("is_chiang_mai", F.when(F.col("location") == "Chiang Mai", 1).otherwise(0))
)

kept = (
    filtered
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.is_remote")), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.job_title").isin(["Analyst", "Director"])), "left_semi")
)

with_windows = (
    eligible
    .withColumn("budget_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("location_min_is_chiang_mai", F.min("is_chiang_mai").over(w2))
)

result = (
    with_windows
    .select(
        "id",
        "dept_name",
        F.col("budget_cume_dist").alias("BUDGET_CUME_DIST_VAL"),
        "budget",
    )
    .unpivot(
        ["id", "dept_name"],
        ["BUDGET_CUME_DIST_VAL", "budget"],
        "metric",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
