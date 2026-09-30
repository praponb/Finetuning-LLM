from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("dept_name") == "Legal"))
    .withColumn("location_group", F.when(F.col("location").isin(["New York", "Tokyo"]), "Group A").otherwise("Other"))
)

kept = (
    base
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (~F.col("x.is_remote")), "left_anti")
)

metrics = (
    kept
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.last("budget", ignorenulls=True).over(w2).alias("last_known_budget"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.email").isNull()), "left_anti")
)

result = (
    not_flagged
    .groupBy("dept_name")
    .agg(
        F.count_if(F.col("location") == "Chiang Mai").alias("chiang_mai_events"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("chiang_mai_events").desc_nulls_last())
)
