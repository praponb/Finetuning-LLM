from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Tokyo", "Berlin", "Singapore"]))
    .orderBy("country", "id")
    .limit(5)
)
