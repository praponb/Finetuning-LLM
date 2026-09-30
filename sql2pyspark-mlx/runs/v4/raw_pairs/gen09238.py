from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name", "location")
w_grp = Window.partitionBy("location", "dept_name")
w_ordered = Window.orderBy(F.col("budget").asc_nulls_last())

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("location") == "Singapore"))
    .select(
        "*",
        F.upper("location").alias("location_upper"),
        F.coalesce("location", F.lit("OTHER")).alias("location_filled"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.manager_id").isNotNull()), "left_semi")
)

latest = (
    not_flagged
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

matched_keys = (
    latest
    .select("id")
    .exceptAll(
        df_departments
        .filter(F.col("budget") < 1945000)
        .select("id")
    )
)

verified = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
    .withColumn("location_sum_budget", F.sum("budget").over(w_grp))
    .withColumn("budget_rank", F.rank().over(w_ordered))
)
