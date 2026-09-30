from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-5, Window.currentRow)

scored = (
    df_departments
    .filter(F.col("location").isin(["New York", "Chiang Mai", "London"]))
    .select(
        "id",
        "location",
        F.lead("budget", 2).over(w).alias("next_budget"),
        F.avg("budget").over(w2).alias("budget_avg_5"),
    )
)

result = (
    scored
    .select(
        "id",
        "location",
        "budget_avg_5",
        F.when(F.col("location").rlike("^(Chiang Mai|London)$"), "CORE").otherwise("OTHER_ZONE").alias("location_zone"),
    )
    .orderBy(F.col("id").desc())
)
