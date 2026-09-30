from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").isNotNull() & (F.col("sqft") != 9153) & F.col("city").isin(["Tokyo", "Bangkok", "Singapore"]))
)
