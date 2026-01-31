FROM apache/spark:4.0.1

USER root

# venv/ensurepip + utilidades mínimas (ca-certificates por si pip/https)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-venv \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# (Opcional pero recomendable) pip al día
RUN python3 -m pip install --upgrade pip

# Paquetes que quieres "horneados" en la imagen (sin reinstalar en runtime)
RUN python3 -m pip install \
    jupyterlab \
    ipykernel \
    pyspark==4.0.1 \
    great-expectations==0.18.22 \
    plotly

# Kernel visible en Jupyter
RUN python3 -m ipykernel install --prefix=/usr/local \
    --name pyspark --display-name "PySpark (Docker)"

WORKDIR /opt/notebooks
EXPOSE 8888

CMD ["bash","-lc","python3 -m jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --ServerApp.token= --ServerApp.password="]
