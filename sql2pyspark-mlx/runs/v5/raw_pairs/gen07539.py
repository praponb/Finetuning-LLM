from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary")
w2 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .withColumn("level_full_name_key", F.concat_ws("/", "level", "full_name"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.rank().over(w).alias("salary_rank"),
        F.first("level", ignorenulls=True).over(w2).alias("first_known_level"),
    )
)

remaining_keys = (
    enriched
    .select("manager_id", "id")
    .subtract(
        df_employees
        .filter(F.col("level") == "L5")
        .select("manager_id", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .select(
        "s.email",
        "s.dept_id",
        "s.manager_id",
        "s.job_title",
        "s.is_remote",
        "s.first_known_level",
        F.col("p.budget"),
        F.col("p.dept_name"),
        F.col("p.location"),
    )
)

cleaned_employees = (
    joined
    .filter(F.col("manager_id").isNotNull())
    .withColumn("location_filled", F.coalesce("location", F.lit("N/A")))
    .withColumn("dept_name_title", F.initcap(F.trim("dept_name")))
    .withColumn("resolved_job_title", F.coalesce("job_title", F.lit("UNKNOWN")))
)

filtered = (
    cleaned_employees
    .filter(F.col("budget") < 3258000)
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
)

result = (
    filtered
    .groupBy(
        "manager_id",
        "dept_name",
        F.when(F.col("budget") >= 3523000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_rows"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .filter(F.col("new_york_rows") >= 1)
)
