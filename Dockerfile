FROM python:3.9.10

WORKDIR /usr/src/app

EXPOSE 8008

ENV PYTHONUNBUFFERED=1

#ADD https://dl.cacerts.digicert.com/DigiCertGlobalRootCA.crt.pem /usr/src/app/
#ADD https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem /usr/src/app/
ADD https://www.digicert.com/CACerts/BaltimoreCyberTrustRoot.crt.pem /usr/src/app/

COPY requirements.txt .
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host=files.pythonhosted.org -r requirements.txt

COPY . .
ENV ENV_SECRETS_PATH="/csi-app-secrets"

CMD exec uvicorn main:app --host 0.0.0.0 --port 8008 --workers 3
#CMD exec gunicorn --log-level info --worker-class=uvicorn.workers.UvicornH11Worker --workers=8 --bind 0.0.0.0:8008 --timeout 600 main:app
