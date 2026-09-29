from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & ~F.col("location").isin(["Tokyo", "Bangkok"]))
    .select("*", F.initcap(F.trim("location")).alias("location_title"))
)

result = (
    filtered
    .groupBy("dept_name")
    .pivot("location", ["Berlin", "London"])
    .agg(F.max("budget").alias("total"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "dept_name",
        F.col("Berlin_total").alias("berlin_total"),
        F.col("Berlin_item_count").alias("berlin_item_count"),
        F.col("London_total").alias("london_total"),
        F.col("London_item_count").alias("london_item_count"),
    )
)
