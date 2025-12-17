FROM astrocrpublic.azurecr.io/runtime:3.1-3

# Enable XCom pickling for astro-sdk-python
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True