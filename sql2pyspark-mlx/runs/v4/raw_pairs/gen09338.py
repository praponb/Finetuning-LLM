from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.upper("email").like("%ARO%"))
    .select(
        "id",
        "segment",
        "signup_date",
        "credit_limit",
        F.row_number().over(w).alias("signup_date_row"),
        F.lead("credit_limit", 1, 0).over(w).alias("next_credit_limit"),
    )
    .orderBy("credit_limit")
)
