from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("location_length").asc_nulls_last())

base = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .withColumn("location_length", F.length("location"))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("DIRECT")))
)

latest = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

kept = (
    latest
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.job_title") == "Director"), "left_anti")
)

kept_departments = (
    kept
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id").isin([2, 29, 37])), "left_semi")
)

not_flagged = (
    kept_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (~F.col("x.level").isin(["L5", "L4"])), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "location",
        "budget",
        "resolved_dept_name",
        F.when(F.col("location").rlike("^(Berlin|New York)$"), "GROUP_A").when(F.col("location").rlike("^(Bangkok|Chiang Mai)$"), "CORE").otherwise("PRIORITY").alias("location_zone"),
    )
    .orderBy(F.col("id").desc())
)
