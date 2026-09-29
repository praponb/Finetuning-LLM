from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "TH", "SG"]) & ((F.col("rating") > 3.8) | F.col("rating").between(3.3, 3.7)))
    .withColumn("supplier_name_country_key", F.concat_ws("-", "supplier_name", "country"))
    .withColumn("rating_int", F.col("rating").cast("int"))
)
