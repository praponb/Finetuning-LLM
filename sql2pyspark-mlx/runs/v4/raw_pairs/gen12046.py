from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("dept_name", "location").orderBy("budget")
w3 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-6, -1)

cleaned = (
    df_departments
    .filter(F.col("budget").between(2065000, 3056000))
    .select("*", F.lower("dept_name").alias("dept_name_lower"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.avg("budget").over(w).alias("budget_avg_2"),
        F.dense_rank().over(w2).alias("budget_dense_rank"),
        F.avg("budget").over(w3).alias("budget_avg_6_prior"),
    )
)

kept_keys = (
    metrics
    .select("dept_name")
    .subtract(
        df_departments
        .filter(F.col("dept_name") == "Finance")
        .select("dept_name")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

by_type = (
    reconciled
    .groupBy("dept_name")
    .pivot("location", ["Singapore", "Chiang Mai"])
    .agg(
        F.max("budget_avg_6_prior").alias("total"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "dept_name",
        F.col("Singapore_total").alias("singapore_total"),
        F.col("Singapore_item_count").alias("singapore_item_count"),
        F.col("Chiang Mai_total").alias("chiang_mai_total"),
        F.col("Chiang Mai_item_count").alias("chiang_mai_item_count"),
    )
)

result = (
    by_type
    .select(
        "dept_name",
        "singapore_total",
        "singapore_item_count",
        "chiang_mai_total",
        "chiang_mai_item_count",
    )
    .orderBy("singapore_total")
)
