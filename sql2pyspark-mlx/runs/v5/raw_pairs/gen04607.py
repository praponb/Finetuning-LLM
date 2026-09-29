from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("location").orderBy("budget", "id")

metrics = (
    df_departments
    .filter(F.col("dept_name") == "Engineering")
    .select(
        "*",
        F.lag("budget").over(w_part).alias("prev_budget"),
        F.lead("budget").over(w_part).alias("next_budget"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

result = (
    metrics
    .withColumn("next_budget_tier", F.when(F.col("next_budget") >= 4277000, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["next_budget_tier", "location"], ["next_budget_tier"], []],
        "next_budget_tier",
        "location",
    )
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.percentile_approx("next_budget", [0.25, 0.5, 0.75]).alias("next_budget_quartiles"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.grouping_id().alias("gid"),
    )
)
