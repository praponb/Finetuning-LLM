from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter((~F.col("location").isin(["Singapore", "New York", "Tokyo"]) & F.col("location").isin(["London", "Chiang Mai"])) | (F.col("location") == "New York"))
    .withColumn("location_lower", F.lower("location"))
)

agg = (
    valid_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["HR", "Engineering"]), "tier_1").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Sales", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sales"),
    )
)

result = agg.orderBy(F.col("pct_sales").desc())
