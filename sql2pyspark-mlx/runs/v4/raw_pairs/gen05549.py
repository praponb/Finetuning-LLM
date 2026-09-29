from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc())
w3 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc(), "id")
w4 = Window.partitionBy("location")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.when(F.col("budget") >= 4326000, "High").when(F.col("budget") >= 811000, "Medium").otherwise("Low").alias("budget_band"),
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w).alias("budget_dense_rank"),
        F.rank().over(w2).alias("budget_rank"),
        F.ntile(4).over(w3).alias("budget_bucket"),
        F.round((F.col("budget") / F.sum("budget").over(w4)), 4).alias("budget_share"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.email").isNotNull()), "left_semi")
)

cleaned = (
    kept
    .filter(F.col("dept_name").isNotNull() & (F.col("budget") >= 3343000))
    .select(
        "id",
        "location",
        "budget",
        "budget_band",
        "location_filled",
        "budget_dense_rank",
        "budget_rank",
        "budget_bucket",
        "budget_share",
        F.ceil("budget_share").alias("budget_share_whole"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("dept_name_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "location",
        "budget_band",
        "budget_rank",
        "budget_bucket",
        "budget_share",
        "budget_share_whole",
        "dept_name_filled",
        F.when(F.col("location").rlike("^(New York|Singapore)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("location_zone"),
    )
)
