from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first(), "id")
w_hist = Window.partitionBy("location")
w3 = Window.orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_departments
    .filter(F.col("location").isNotNull() & F.col("location").isin(["New York", "Singapore", "Tokyo"]))
    .withColumn("location_prefix", F.substring("location", 1, 2))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("OTHER")))
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.job_title") == "Director"), "left_anti")
)

metrics = (
    eligible
    .select(
        "*",
        F.count("budget").over(w_hist).alias("location_count_budget"),
        F.first("location", ignorenulls=True).over(w3).alias("first_known_location"),
    )
)

result = (
    metrics
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "location_prefix",
        "resolved_dept_name",
        "latest_rank",
        "first_known_location",
        F.when(F.col("location").rlike("^(Singapore|Tokyo)$"), "GROUP_A").when(F.col("location").rlike("^(Bangkok|Chiang Mai)$"), "EXTENDED").otherwise("PRIORITY").alias("location_zone"),
    )
    .orderBy(F.col("budget").desc_nulls_first())
)
