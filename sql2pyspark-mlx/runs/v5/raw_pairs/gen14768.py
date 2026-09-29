from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "job_title").orderBy(F.col("hire_date").desc_nulls_first())
w2 = Window.partitionBy("level", "job_title").orderBy(F.col("salary").desc(), "id")

filtered = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.trim("level").alias("clean_level"),
        F.coalesce("job_title", F.lit("OTHER")).alias("resolved_job_title"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

cleaned = (
    latest
    .filter(F.col("job_title").isNotNull() & (F.col("hire_date") >= "2025-10-01"))
    .select(
        "id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.add_months("hire_date", 3).alias("hire_date_plus_months"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

matched_keys = (
    deduped
    .select("level", "job_title")
    .intersectAll(
        df_employees
        .filter(F.col("level") == "L3")
        .select("level", "job_title")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

grouped = (
    kept_rows
    .groupBy(
        F.when(F.col("salary") >= 125000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_rows"),
    )
)

result = (
    grouped
    .select("salary_tier", "job_title_list", "designer_rows")
    .orderBy(F.col("salary_tier").desc_nulls_first())
)
