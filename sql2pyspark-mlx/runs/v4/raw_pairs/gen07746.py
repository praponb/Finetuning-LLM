from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .filter(F.upper(F.trim("email")) == "KANYA@MAIL.CO.TH")
    .select("id", "segment", "signup_date", F.rank().over(w).alias("signup_date_rank"))
    .orderBy(F.col("id").desc())
)
