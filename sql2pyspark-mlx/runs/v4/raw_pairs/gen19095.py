from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")

ranked = (
    df_departments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    ranked
    .filter(F.col("dept_name").isin(["HR", "Engineering"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "rank_in_group",
        F.col("rank_in_group").cast("string").alias("rank_in_group_str"),
    )
)

remaining_keys = (
    base
    .select("id", "dept_name")
    .intersect(
        df_departments
        .filter(F.col("budget") < 2596000)
        .select("id", "dept_name")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "dept_name", "budget", "rank_in_group", "rank_in_group_str")
    .orderBy("dept_name")
)
