from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(3092000, 3287000) & (F.col("budget") < 1996000))
    .groupBy(
        F.when(F.col("location").isin(["Tokyo", "London"]), "group_a").otherwise("other").alias("location_group"),
    )
    .agg(
        F.count("*").alias("department_count"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .orderBy(F.col("location_group").desc())
    .limit(1)
)
