from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Berlin") & (F.col("budget") >= 3064000) & (F.col("budget") > 2258000))
    .select(
        "id",
        "dept_name",
        "location",
        F.when(F.col("dept_name") == "Finance", 1).otherwise(0).alias("is_finance"),
    )
)
