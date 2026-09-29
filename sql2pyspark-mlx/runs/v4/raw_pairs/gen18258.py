from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget_per_sales").desc())

matched_keys = (
    df_departments
    .select("id", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("budget") < 3637000)
        .select("id", "dept_name")
    )
)

reconciled = (
    df_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3118000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Sales", 1)), F.lit(0)), 2).alias("budget_per_sales"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Finance", 1)), F.lit(0)), 2).alias("budget_per_finance"),
    )
    .withColumn("budget_per_sales_percent_rank", F.percent_rank().over(w))
)
