from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget_whole").desc(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc_nulls_last())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.ceil("budget").alias("budget_whole"),
        F.concat("dept_name", F.lit(" - "), "location").alias("dept_name_location"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_filled"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "location",
        "dept_name_location",
        "location_filled",
        F.ntile(5).over(w).alias("budget_whole_bucket"),
        F.ntile(10).over(w2).alias("budget_bucket"),
        F.rank().over(w3).alias("budget_rank"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.dept_name_location",
        F.col("c.level").alias("employee_level"),
    )
)

filtered_departments = (
    with_latest
    .filter(F.col("employee_level").isNotNull() & (~F.col("location").eqNullSafe(F.lit("Berlin"))))
    .select(
        "*",
        F.substring("location", 1, 3).alias("location_prefix"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("location_filled"),
    )
)

kept = (
    filtered_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.full_name").like("Iv%")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "location",
        "dept_name_location",
        "employee_level",
        F.when(F.col("location").rlike("^(New York|Bangkok)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("location_zone"),
    )
    .orderBy(F.col("location_zone").asc_nulls_first(), "employee_level")
)
