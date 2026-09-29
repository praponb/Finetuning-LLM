from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc())

base = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-08-17"))) & (~F.col("level").eqNullSafe(F.lit("L2"))))
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.substring("full_name", 1, 4).alias("full_name_prefix"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_clean"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    ranked
    .filter(~F.col("job_title").eqNullSafe(F.lit("Designer")))
    .select(
        "id",
        "dept_id",
        "job_title",
        "hire_date",
        "full_name_prefix",
        "latest_rank",
        F.substring("job_title", 1, 4).alias("job_title_prefix"),
    )
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

matched_keys = (
    latest
    .select("id", "dept_id")
    .intersectAll(
        df_employees
        .filter(F.col("salary") <= 60000)
        .select("id", "dept_id")
    )
)

verified = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        "e.dept_id",
        "e.hire_date",
        "e.full_name_prefix",
        "e.job_title_prefix",
        F.col("d.location"),
    )
)

valid = (
    joined
    .filter((F.col("hire_date") > F.lit("2023-07-20").cast("date")) & F.col("dept_id").isin([3, 8, 31, 33]))
    .withColumn("hire_date_month_end", F.last_day("hire_date"))
    .withColumn("location_filled", F.coalesce("location", F.lit("DIRECT")))
)

result = (
    valid
    .select(
        "location",
        "hire_date_month_end",
        "location_filled",
        F.when(F.col("location").rlike("^(Chiang Mai|Berlin)$"), "GROUP_B").when(F.col("location").rlike("^(Bangkok|New York)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("location_zone"),
    )
)
