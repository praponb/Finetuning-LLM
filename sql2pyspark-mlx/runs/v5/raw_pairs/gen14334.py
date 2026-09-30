from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w_part = Window.partitionBy("location")

remaining_keys = (
    df_departments
    .select("id", "dept_name")
    .subtract(
        df_departments
        .filter(~F.col("dept_name").isin(["HR", "Sales"]))
        .select("id", "dept_name")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w)), 4))
    .withColumn("budget_share_2", F.round((F.col("budget") / F.sum("budget").over(w)), 4))
    .withColumn("location_stddev_samp_budget", F.stddev_samp("budget").over(w_part))
)

valid = (
    enriched
    .filter(F.col("location").isin(["Bangkok", "Chiang Mai"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget_share",
        "budget_share_2",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.coalesce("location", F.lit("NONE")).alias("resolved_location"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.full_name").like("I%")), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.manager_id").isNotNull()), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("budget_filled") >= 61, "HIGH").otherwise("LOW").alias("budget_filled_tier"),
        "location",
    )
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget_share"))).alias("sales_total_budget_share"),
    )
    .filter(F.col("sales_total_budget_share") >= 0)
    .orderBy(F.col("budget_filled_tier").desc())
)
