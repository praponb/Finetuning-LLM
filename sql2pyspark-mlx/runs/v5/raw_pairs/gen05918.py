from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").asc_nulls_first(), "id")

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("resolved_dept_name"),
    )
)

valid = (
    base
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.round((F.col("budget") * 1.15), 2).alias("adjusted_budget"),
        F.round("budget", 2).alias("budget_rounded"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "dept_name", "location", "location_filled", "resolved_dept_name")
)

eligible = (
    ranked
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (~F.col("c.level").isin(["L1", "L3", "L5"])), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "dept_name",
        "location",
        "location_filled",
        "resolved_dept_name",
        F.when(F.col("dept_name").rlike("^(Legal|Finance)$"), "EXTENDED").otherwise("CORE").alias("dept_name_zone"),
    )
    .orderBy(F.col("location").desc_nulls_last())
)
