from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Finance", "Legal"]))
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(" - "), "location"))
)

result = (
    base
    .groupBy("dept_name")
    .pivot("location", ["Chiang Mai", "London"])
    .agg(F.sum("budget"))
    .select(
        "dept_name",
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("London").alias("london"),
    )
)
