# DataOS Pipeline - GPU-enabled container
# Based on HuggingFace Transformers with PyTorch GPU support

FROM huggingface/transformers-pytorch-gpu:4.35.2

WORKDIR /opt/prefect

# Install Python dependencies
COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir \
    tqdm thefuzz numpy nltk flair openai keybert \
    grpcio tenacity spacy clickhouse-driver clickhouse-connect \
    python-dotenv pandas prefect>=2.12.0

# Install spaCy with CUDA support
RUN python3 -m pip install --no-cache-dir spacy[cuda118] spacy-transformers

# Download spaCy models
RUN python3 -m spacy download en_core_web_lg --quiet && \
    python3 -m spacy download en_core_web_trf --quiet

# Copy application code
COPY flows /opt/prefect/flows
COPY deployments.py /opt/prefect/

# Default command - start Prefect agent
# Override work pool name via environment or docker-compose
CMD ["prefect", "agent", "start", "--pool", "gpu-agents", "--limit", "1"]
