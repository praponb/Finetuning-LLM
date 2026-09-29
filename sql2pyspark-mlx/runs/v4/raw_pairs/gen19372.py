from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("hire_date", "id")

valid = (
    df_employees
    .filter(F.col("manager_id") == 34)
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "level",
        F.trim("level").alias("clean_level"),
    )
)

cleaned = (
    valid
    .filter(F.col("level").isNotNull())
    .withColumn("full_name_lower", F.lower("full_name"))
    .withColumn("full_name_upper", F.upper("full_name"))
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "inner")
    .select(
        "e.*",
        F.col("d.hire_date"),
        F.col("d.full_name").alias("employee_full_name"),
        F.col("d.email"),
    )
)

enriched = (
    with_dims
    .select(
        "*",
        F.last("level", ignorenulls=True).over(w).alias("last_known_level"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

result = (
    enriched
    .groupBy("manager_id")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_rows"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
    .filter(F.col("manager_rows") > 1)
)
