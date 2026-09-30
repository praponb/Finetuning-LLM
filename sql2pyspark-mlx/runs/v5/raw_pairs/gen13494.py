from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["London", "Bangkok", "Singapore"]))
    .select("region")
    .distinct()
    .orderBy("region")
    .limit(20)
)
