from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")

base = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .withColumn("budget_tier", F.when(F.col("budget") >= 3069000, "High").when(F.col("budget") >= 1706000, "Medium").otherwise("Low"))
    .withColumn("budget_band", F.when(F.col("budget") >= 4219000, "High").when(F.col("budget") >= 1124000, "Medium").otherwise("Low"))
)

result = (
    base
    .select(
        "*",
        F.last("location", ignorenulls=True).over(w).alias("last_known_location"),
        F.ntile(10).over(w_hist).alias("budget_bucket"),
    )
)
