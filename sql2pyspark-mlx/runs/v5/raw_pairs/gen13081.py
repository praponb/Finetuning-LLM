from pyspark.sql import functions as F

src = df_departments.filter(F.col("budget") <= 3232000)

agg = (
    src
    .groupBy("dept_name")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
    )
)

result = (
    agg
    .filter(F.col("pct_tokyo") > 33)
    .orderBy(F.col("pct_tokyo").desc())
)
