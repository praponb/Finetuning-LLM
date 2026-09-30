from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.round((F.col("budget") * 0.9), 2).alias("budget_scaled"),
        F.coalesce("dept_name", F.lit("NONE")).alias("resolved_dept_name"),
    )
)

result = (
    filtered
    .groupBy("dept_name")
    .pivot("location", ["Singapore", "London"])
    .agg(F.sum("budget").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "dept_name",
        F.col("Singapore_total").alias("singapore_total"),
        F.col("Singapore_cnt").alias("singapore_cnt"),
        F.col("London_total").alias("london_total"),
        F.col("London_cnt").alias("london_cnt"),
    )
)
