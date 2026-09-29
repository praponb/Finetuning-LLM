from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

ranked = (
    df_departments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3760000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("budget")).otherwise(0)).alias("marketing_budget"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "Singapore", 1)), F.lit(0)), 2).alias("budget_per_singapore"),
    )
    .orderBy(F.col("budget_tier").desc_nulls_last())
)
