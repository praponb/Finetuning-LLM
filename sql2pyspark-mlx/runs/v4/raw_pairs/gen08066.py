from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("budget") < 2399000)
    .select(
        "*",
        F.substring("dept_name", 1, 3).alias("dept_name_prefix"),
        F.concat("dept_name", F.lit(" "), "location").alias("dept_name_location"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("location_clean"),
    )
)

cleaned = (
    valid
    .filter(F.col("dept_name").isNotNull() & F.col("location").isin(["London", "New York", "Chiang Mai"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "dept_name_prefix",
        "location_clean",
        F.lower("dept_name").alias("dept_name_lower"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("dept_name_filled"),
    )
)

valid_departments = (
    cleaned
    .filter(F.col("location").isNotNull())
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)

not_flagged = (
    valid_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary").between(112000, 122000)), "left_anti")
)

result = (
    not_flagged
    .groupBy("dept_name")
    .pivot("location", ["London", "Chiang Mai", "Singapore"])
    .agg(F.max("budget_filled").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "dept_name",
        F.col("London_total").alias("london_total"),
        F.col("London_cnt").alias("london_cnt"),
        F.col("Chiang Mai_total").alias("chiang_mai_total"),
        F.col("Chiang Mai_cnt").alias("chiang_mai_cnt"),
        F.col("Singapore_total").alias("singapore_total"),
        F.col("Singapore_cnt").alias("singapore_cnt"),
    )
)
