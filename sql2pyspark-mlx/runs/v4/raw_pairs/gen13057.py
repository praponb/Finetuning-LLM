from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w4 = Window.partitionBy("location", "dept_name").orderBy(F.col("location_max_budget").desc(), "id")

enriched = (
    df_departments
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.max("budget").over(w2).alias("location_max_budget"),
        F.ntile(10).over(w3).alias("budget_bucket"),
    )
)

cleaned = (
    enriched
    .filter(F.col("location").isin(["New York", "Bangkok"]))
    .withColumn("location_dept_name", F.concat("location", F.lit("/"), "dept_name"))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date") <= "2025-08-01"), "left_anti")
)

cleaned_departments = (
    eligible
    .filter(F.col("budget_bucket") != 8)
    .select(
        "*",
        F.when(F.col("location_max_budget") >= 3356247, "High").when(F.col("location_max_budget") >= 1483713, "Medium").otherwise("Low").alias("location_max_budget_tier"),
        F.coalesce("location", F.lit("DIRECT")).alias("resolved_location"),
    )
)

latest = (
    cleaned_departments
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "location_max_budget",
        "budget_bucket",
        "location_max_budget_tier",
        "resolved_location",
        "latest_rank",
        F.when(F.col("location").rlike("^(Chiang Mai|Berlin)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("location_zone"),
    )
    .orderBy(F.col("location").desc())
)
