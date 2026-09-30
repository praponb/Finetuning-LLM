from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w2 = Window.orderBy("dedup_rank", "id")
w3 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("budget", "id")

filtered = (
    df_departments
    .filter(F.col("location") != "New York")
    .select(
        "*",
        F.when(F.col("location") == "Bangkok", 1).otherwise(0).alias("is_bangkok"),
        F.coalesce("location", F.lit("NONE")).alias("resolved_location"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    ranked
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("cumulative_budget", F.sum("budget").over(w3))
    .withColumn("budget_bucket", F.ntile(5).over(w4))
)

result = (
    scored
    .select(
        "id",
        "location",
        "budget",
        "is_bangkok",
        "dedup_rank",
        "event_seq",
        "cumulative_budget",
        "budget_bucket",
    )
)
