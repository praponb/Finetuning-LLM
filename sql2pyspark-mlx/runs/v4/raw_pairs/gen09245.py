from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~(~F.col("city").isin(["Singapore", "London", "New York"]) & (F.col("rating") < 2.3)))
    .select("supplier_name")
    .orderBy("supplier_name")
)
