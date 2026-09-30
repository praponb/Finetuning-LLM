from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_last(), "id")
w3 = Window.orderBy(F.col("budget").asc_nulls_first())
w4 = Window.partitionBy("dept_name")
w5 = Window.partitionBy("location", "dept_name")

latest = (
    df_departments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

deduped = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

filtered = (
    deduped
    .filter(F.col("location").isNotNull() & (F.col("dept_name") == "Legal"))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "rn",
        F.upper("dept_name").alias("dept_name_upper"),
        F.coalesce("location", F.lit("OTHER")).alias("resolved_location"),
    )
)

remaining_keys = (
    filtered
    .select("dept_name", "id")
    .intersect(
        df_departments
        .filter(F.col("location") == "Singapore")
        .select("dept_name", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("budget_cume_dist", F.cume_dist().over(w3))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w4)), 4))
    .withColumn("location_stddev_samp_budget", F.stddev_samp("budget").over(w5))
)
