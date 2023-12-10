# We're using the latest version of Prefect with Python 3.10
FROM prefecthq/prefect:2-python3.10

# Add our requirements.txt file to the image and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir
RUN python -m spacy download en_core_web_sm
RUN python -m spacy download en_core_web_trf

# Add our flow code to the image
COPY flows /opt/prefect/flows
COPY deployments.py /opt/prefect

# Run our flow script when the container starts
CMD prefect agent start --pool "vast-agents" --limit 1