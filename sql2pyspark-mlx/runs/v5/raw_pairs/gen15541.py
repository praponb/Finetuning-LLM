from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_last())
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location")

scored = (
    df_departments
    .filter(F.col("budget") >= 3199000)
    .select(
        "id",
        "location",
        F.rank().over(w).alias("budget_rank"),
        F.sum("budget").over(w2).alias("cumulative_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w3)), 4).alias("budget_share"),
    )
)

result = (
    scored
    .select(
        "id",
        "location",
        "budget_rank",
        "cumulative_budget",
        F.when(F.col("location").rlike("^(Chiang Mai|London)$"), "EXTENDED").otherwise("PRIORITY").alias("location_zone"),
    )
    .orderBy("location_zone")
)
