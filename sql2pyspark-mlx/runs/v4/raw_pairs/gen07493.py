from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_last())
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_first())
w3 = Window.partitionBy("dept_name").orderBy("latest_rank", "id")
w4 = Window.orderBy("latest_rank", "id").rowsBetween(-5, -1)

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.round((F.col("budget") * 100), 2).alias("budget_scaled"),
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.lower(F.trim(F.col("x.email"))) == "david.lee@gmail.com"), "left_anti")
)

result = (
    not_flagged
    .withColumn("budget_cume_dist", F.cume_dist().over(w2))
    .withColumn("next_budget_scaled", F.lead("budget_scaled", 1).over(w3))
    .withColumn("budget_scaled_avg_5_prior", F.avg("budget_scaled").over(w4))
)
