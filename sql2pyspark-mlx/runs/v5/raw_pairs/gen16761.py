from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-6, -1)
w_hist = Window.partitionBy("location", "dept_name").orderBy("location_length_avg_6_prior")
w4 = Window.partitionBy("location")
w5 = Window.partitionBy("dept_name").orderBy(F.col("location_length_avg_6_prior").asc_nulls_first())

filtered = (
    df_departments
    .filter(F.col("budget").between(2147000, 3843000))
    .select(
        "*",
        F.length("location").alias("location_length"),
        F.trim("location").alias("clean_location"),
        F.coalesce("dept_name", F.lit("NONE")).alias("dept_name_filled"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.first("dept_name", ignorenulls=True).over(w_part).alias("first_known_dept_name"),
        F.avg("location_length").over(w2).alias("location_length_avg_6_prior"),
    )
)

remaining_keys = (
    metrics
    .select("dept_name")
    .subtract(
        df_departments
        .filter(F.col("budget") < 3618000)
        .select("dept_name")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .filter(F.col("budget") <= 3484000)
    .select(
        "id",
        "location",
        "location_length_avg_6_prior",
        F.cume_dist().over(w_hist).alias("location_length_avg_6_prior_cume_dist"),
        F.sum("location_length").over(w4).alias("location_sum_location_length"),
        F.cume_dist().over(w5).alias("location_length_avg_6_prior_cume_dist_2"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.dept_id") == 13), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.level").isNotNull()), "left_anti")
)

result = (
    kept
    .groupBy("location")
    .agg(
        F.countDistinct("location").alias("distinct_locations"),
        F.count_if(F.col("location") == "London").alias("london_events"),
        F.round(F.sum("location_length_avg_6_prior_cume_dist") / F.nullif(F.count(F.when(F.col("location") == "Berlin", 1)), F.lit(0)), 2).alias("location_length_avg_6_prior_cume_dist_per_berlin"),
        F.sum(F.when(F.col("location") == "New York", F.col("location_length_avg_6_prior_cume_dist")).otherwise(0)).alias("new_york_location_length_avg_6_prior_cume_dist"),
    )
)
