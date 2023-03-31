FROM prefecthq/prefect:2-python3.10
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir
ADD app /opt/prefect/flows
COPY .env /opt/prefect/flows/.env