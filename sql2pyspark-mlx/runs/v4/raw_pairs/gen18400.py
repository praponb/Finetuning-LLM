from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("location").orderBy("budget", "id")
w3 = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy("budget", "id")
w5 = Window.partitionBy("location").orderBy(F.col("row_seq").desc_nulls_first(), "id")
w6 = Window.partitionBy("dept_name").orderBy("dedup_rank", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy("dept_name").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("budget") < 2671000))
    .select(
        "*",
        F.when(F.col("dept_name").isin(["Sales", "Finance"]), "Priority").otherwise("Other").alias("dept_name_group"),
        F.coalesce("location", F.lit("NONE")).alias("location_clean"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.first("location", ignorenulls=True).over(w).alias("first_known_location"),
        F.row_number().over(w_part).alias("row_seq"),
        F.last("location", ignorenulls=True).over(w3).alias("last_known_location"),
        F.row_number().over(w4).alias("row_seq_2"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.upper(F.trim(F.col("c.full_name"))) == "HENRY FORD"), "left_anti")
)

deduped = (
    eligible
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

enriched_departments = (
    deduped
    .withColumn("row_seq_2_avg_2_prior", F.avg("row_seq_2").over(w6))
    .withColumn("row_seq_avg_2_prior", F.avg("row_seq").over(w7))
    .withColumn("last_known_row_seq", F.last("row_seq", ignorenulls=True).over(w8))
)

with_last = (
    enriched_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"))
)

result = (
    with_last
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "dept_name_group",
        "first_known_location",
        "row_seq_2",
        "dedup_rank",
        "row_seq_2_avg_2_prior",
    )
    .orderBy("location", F.col("dept_name").desc())
)
