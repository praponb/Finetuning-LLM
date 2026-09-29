from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_group", F.when(F.col("location").isin(["Chiang Mai", "Tokyo"]), "Group A").otherwise("Other"))
)

eligible = (
    base
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id") == 25), "left_anti")
)

eligible_departments = (
    eligible
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.dept_id").isin([6, 18])), "left_anti")
)

matched_keys = (
    eligible_departments
    .select("dept_name")
    .exceptAll(
        df_departments
        .filter(F.col("dept_name").isin(["Legal", "Engineering", "HR"]))
        .select("dept_name")
    )
)

kept_rows = (
    eligible_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

stats = (
    kept_rows
    .groupBy("location")
    .agg(
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_rows"),
        F.avg(F.when(F.col("location") == "London", F.col("budget"))).alias("london_avg_budget"),
        F.count("*").alias("num_rows"),
        F.max(F.when(F.col("location") == "New York", F.col("budget"))).alias("new_york_max_budget"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Engineering", 1)), F.lit(0)), 2).alias("budget_per_engineering"),
    )
)

result = (
    stats
    .select(
        "location",
        "berlin_rows",
        "london_avg_budget",
        "num_rows",
        "new_york_max_budget",
        "budget_stddev",
    )
    .orderBy(F.col("new_york_max_budget").desc_nulls_last())
)
