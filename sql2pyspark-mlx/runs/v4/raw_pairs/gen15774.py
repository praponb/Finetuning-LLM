from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 3967000) & F.col("location").isin(["Chiang Mai", "Tokyo", "Singapore"]))
    .select(
        "id",
        "location",
        F.round((F.col("budget") * 1.15), 2).alias("budget_adjusted"),
        F.when(F.col("dept_name") == "Marketing", 1).otherwise(0).alias("is_marketing"),
    )
    .orderBy("id")
)
