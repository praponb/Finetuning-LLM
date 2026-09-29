from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote") | F.col("level").isin(["L1", "L2"]))
    .select("id", "level", "full_name")
)
