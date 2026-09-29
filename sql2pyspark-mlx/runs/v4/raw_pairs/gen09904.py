from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "SMB", "Consumer"]))
    .select("id", "age")
    .unionByName(df_customers.filter(F.col("segment") != "SMB").select("id", "age"))
    .distinct()
)
