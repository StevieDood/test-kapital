from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, rank, datediff, current_date
from pyspark.sql.window import Window

def main():

    spark = SparkSession.builder \
        .appName("TransactionValidation").getOrCreate()

    try:
        # Datos de ejemplo (Se incluyen datos de casos de uso posibles!)
        data1 = [
            ("101", "2023-01-01", 100.00, "USD"),  # Válido
            ("102", "2023-01-05", 200.00, "EUR"),  # Válido
            ("101", "2323-01-10", 150.00, "USD"), # Futuro lejano (error)
            ("104", "2010-01-01", 300.00, "GBP"), # Más de 10 años atrás (error)
            ("105", "2025-12-31", 400.00, "CAD")  # Futuro cercano (error)
        ]
        
        data2 = [
            ("101", "2023-01-02", 120.00, "USD"),  # Válido
            ("102", "2023-01-07", 190.00, "EUR"),  # Válido
            ("103", "2323-01-15", 500.00, "USD"), # Futuro lejano (error)
            ("103", "2023-01-15", 500.00, "USD") # Válido
        ]

        columns = ["ID", "Date", "Amount", "Currency"]
        
        df1 = spark.createDataFrame(data1, columns)
        df2 = spark.createDataFrame(data2, columns)

        combined_df = df1.union(df2)
        df_with_date = combined_df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

        today = current_date()
        
        df_with_diff = df_with_date.withColumn("DaysFromToday", datediff(col("Date"), today))

        # Definir condiciones para errores:
        # Fechas futuras (DaysFromToday > 0)
        # Fechas muy antiguas (DaysFromToday < -3650 [10 años])
        error_condition = (col("DaysFromToday") > 0) | (col("DaysFromToday") < -3650)
        
        # Separar en DataFrames válidos e inválidos
        valid_df = df_with_diff.filter(~error_condition)
        error_df = df_with_diff.filter(error_condition).drop("DaysFromToday")

        window_spec = Window.partitionBy("ID").orderBy(col("Date").desc())

        final_df = valid_df.withColumn("rank", rank().over(window_spec)) \
            .filter(col("rank") == 1) \
            .drop("rank", "DaysFromToday") \
            .orderBy("ID")

        print("\n")
        print("DataFrame final con registros válidos más recientes:")
        final_df.show()

        print("\n")
        print("DataFrame con registros con fechas inválidas (errores):")
        error_df.show()


    finally:
        spark.stop()

if __name__ == "__main__":
    main()