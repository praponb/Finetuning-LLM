from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget_filled").desc_nulls_last(), "id")

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
)

cleaned_departments = (
    cleaned
    .filter(F.col("budget") >= 1566000)
    .withColumn("location_group", F.when(F.col("location").isin(["Berlin", "Tokyo"]), "Priority").otherwise("Other"))
)

deduped = (
    cleaned_departments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.manager_id").isin([10, 14, 17, 31])), "left_anti")
)

valid = (
    eligible
    .filter(F.col("location").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "budget_filled",
        "dept_name_title",
        "location_group",
        F.col("dedup_rank").cast("double").alias("dedup_rank_num"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("resolved_dept_name"),
    )
)

result = (
    valid
    .select(
        "id",
        "location",
        "budget_filled",
        "dept_name_title",
        "location_group",
        "dedup_rank_num",
        "resolved_dept_name",
        F.when(F.col("location").rlike("^(New York|Chiang Mai)$"), "CORE").otherwise("OTHER_ZONE").alias("location_zone"),
    )
    .orderBy(F.col("dedup_rank_num").desc(), "location_zone")
)
