from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc_nulls_last())
w2 = Window.partitionBy("dept_name")

remaining_keys = (
    df_departments
    .select("location")
    .intersect(
        df_departments
        .filter(F.col("budget").isNotNull())
        .select("location")
    )
)

reconciled = (
    df_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .filter(F.col("location").isin(["London", "New York", "Bangkok"]))
    .withColumn("budget_percent_rank", F.percent_rank().over(w))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
)

filtered = (
    metrics
    .filter(~F.col("dept_name").eqNullSafe(F.lit("Marketing")))
    .select(
        "*",
        F.when(F.col("location") == "Singapore", 1).otherwise(0).alias("is_singapore"),
        F.lower("dept_name").alias("dept_name_lower"),
        F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_filled"),
    )
)

matched_keys = (
    filtered
    .select("location", "id")
    .intersect(
        df_departments
        .filter(F.col("dept_name").isin(["Legal", "Sales"]))
        .select("location", "id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary").between(78000, 128000)), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "dept_name",
        "location",
        "budget_percent_rank",
        "budget_share",
        "dept_name_lower",
        "dept_name_filled",
    )
)
