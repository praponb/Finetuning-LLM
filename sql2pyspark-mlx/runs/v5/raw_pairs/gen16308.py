from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("dept_name_length").desc_nulls_last())
w2 = Window.orderBy("budget")
w3 = Window.partitionBy("dept_name")
w4 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_last())

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & F.col("dept_name").isin(["Legal", "Finance", "HR"]))
    .select(
        "*",
        F.upper("location").alias("location_upper"),
        F.length("dept_name").alias("dept_name_length"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("resolved_dept_name"),
    )
)

remaining_keys = (
    valid
    .select("location", "id")
    .intersect(
        df_departments
        .filter(F.col("location") != "Singapore")
        .select("location", "id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "dept_name",
        "budget",
        "location_upper",
        "resolved_dept_name",
        F.dense_rank().over(w).alias("dept_name_length_dense_rank"),
        F.dense_rank().over(w2).alias("budget_dense_rank"),
    )
)

enriched_departments = (
    enriched
    .filter(F.col("dept_name") == "Sales")
    .select(
        "dept_name",
        "resolved_dept_name",
        F.round((F.col("budget_dense_rank") / F.sum("budget_dense_rank").over(w3)), 4).alias("budget_dense_rank_share"),
        F.cume_dist().over(w4).alias("budget_cume_dist"),
    )
)

result = (
    enriched_departments
    .select(
        "dept_name",
        "resolved_dept_name",
        "budget_dense_rank_share",
        "budget_cume_dist",
    )
)
