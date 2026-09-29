from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("city_country", F.concat("city", F.lit(" - "), "country"))
    .withColumn("supplier_name_city", F.concat("supplier_name", F.lit("/"), "city"))
)
