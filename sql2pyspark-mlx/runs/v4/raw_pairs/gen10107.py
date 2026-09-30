from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("S%") & F.col("city").isin(["Singapore", "London"]))
    .orderBy(F.col("rating").desc(), F.col("id").desc())
)
