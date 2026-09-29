from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") < 3.6) | F.col("supplier_name").like("%EuroGood%"))
    .select("id", "country", "rating")
    .orderBy(F.col("id").desc(), "rating")
    .limit(50)
)
