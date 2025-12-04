<![CDATA[<div align="center">

# 📊 DataOS Pipeline

### Distributed Data Processing Pipeline for Decentralized Content

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Prefect](https://img.shields.io/badge/orchestration-Prefect-0052CC)](https://www.prefect.io/)
[![Status: Archived](https://img.shields.io/badge/status-archived-red.svg)](#archived-notice)

<br />

**A production-grade ETL pipeline for processing decentralized content from Arweave, featuring NLP metadata extraction, entity recognition, and AI-powered analysis.**

<br />

[Overview](#overview) •
[Architecture](#architecture) •
[Features](#features) •
[Installation](#installation) •
[Usage](#usage) •
[License](#license)

</div>

---

## ⚠️ Archived Notice

> **This project is archived and no longer maintained.**
>
> This repository has been open-sourced for educational and reference purposes. It was developed by Roark Technology as part of internal research and development efforts.
>
> - 🚫 **No support** will be provided
> - 🚫 **No new features** will be added  
> - 🚫 **No bug fixes** will be implemented
> - ⚠️ **Dependencies may be outdated** and contain known vulnerabilities
>
> Use at your own risk. Feel free to fork and adapt for your own purposes.

---

## Overview

DataOS Pipeline is a scalable data processing system designed to:

- **Ingest** content from decentralized storage networks (Arweave)
- **Extract** rich metadata using NLP and machine learning models
- **Process** text with entity recognition, categorization, and embeddings
- **Store** results in a ClickHouse analytical database

The pipeline leverages [Prefect](https://www.prefect.io/) for orchestration, supporting both CPU and GPU workloads with automatic scaling and retry logic.

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Data Source   │────▶│    Pipeline     │────▶│   Data Store    │
│    (Arweave)    │     │   (Prefect)     │     │  (ClickHouse)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
              ┌─────▼─────┐         ┌─────▼─────┐
              │ CPU Tasks │         │ GPU Tasks │
              │  • NER    │         │ • Whisper │
              │  • BART   │         │ • spaCy   │
              │  • KeyBERT│         │ • Flair   │
              └───────────┘         └───────────┘
```

---

## Architecture

### Components

| Component | Description |
|-----------|-------------|
| **`flows/`** | Prefect flow definitions for data pipelines |
| **`flows/util/`** | Shared utilities for NLP, database, and API operations |
| **`sql/`** | ClickHouse schema definitions and analytical queries |
| **`deployments.py`** | Prefect deployment configuration |

### Data Flows

1. **Block Ingestion** (`blocks.py`)
   - Fetches Arweave block metadata
   - Tracks blockchain state for incremental processing

2. **Content Download** (`download.py`)
   - Retrieves content from Arweave gateways
   - Implements retry logic with gateway rotation

3. **Text Quality Assessment** (`check_text_quality.py`)
   - Evaluates English language quality
   - Filters content for downstream processing

4. **Metadata Extraction**
   - **CPU Pipeline** (`metadata_cpu.py`): OpenAI embeddings, text summarization
   - **GPU Pipeline** (`metadata_gpu.py`): spaCy NER, Flair NER, BART classification, KeyBERT keywords

5. **Audio/Video Transcription** (`transcription.py`)
   - Whisper-based transcription for video content

---

## Features

### NLP & Machine Learning

| Feature | Model/Library | Description |
|---------|---------------|-------------|
| Named Entity Recognition | spaCy, Flair | Extract people, organizations, locations |
| Zero-shot Classification | BART | Categorize content into topics |
| Keyword Extraction | KeyBERT | Extract key phrases from text |
| Text Embeddings | OpenAI Ada | Generate semantic embeddings |
| Transcription | Faster-Whisper | Convert audio/video to text |
| Language Detection | ClickHouse NLP | Identify content language |

### Infrastructure

- **Orchestration**: Prefect with work pools for CPU/GPU separation
- **Database**: ClickHouse for high-performance analytics
- **Containerization**: Docker with GPU support
- **Scalability**: Distributed worker architecture

---

## Installation

### Prerequisites

- Python 3.10+
- Docker (optional, for containerized deployment)
- NVIDIA GPU + CUDA 11.8 (optional, for GPU acceleration)
- ClickHouse database instance
- Prefect Cloud account (or self-hosted Prefect server)

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-org/dataos-pipeline.git
   cd dataos-pipeline
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/macOS
   # or: venv\Scripts\activate  # Windows
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   
   # For GPU support
   pip install spacy[cuda118]
   python -m spacy download en_core_web_lg
   python -m spacy download en_core_web_trf
   ```

4. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. **Connect to Prefect**
   ```bash
   prefect cloud login  # For Prefect Cloud
   # or configure PREFECT_API_URL for self-hosted
   ```

---

## Usage

### Running Locally

```bash
# Start CPU agent
./run-cpu-agent.sh

# Start GPU agent (requires NVIDIA GPU)
./run-gpu-agent.sh
```

### Docker Deployment

```bash
# Build and start services
docker compose up -d

# Scale workers
docker compose up -d --scale prefect_w1=4
```

### Deploying Flows

```bash
# Register deployments with Prefect
python deployments.py
```

---

## Project Structure

```
dataos-pipeline/
├── flows/
│   ├── __init__.py
│   ├── blocks.py           # Arweave block ingestion
│   ├── check_text_quality.py
│   ├── download.py         # Content download
│   ├── metadata_cpu.py     # CPU-based metadata extraction
│   ├── metadata_gpu.py     # GPU-accelerated metadata extraction
│   ├── transcription.py    # Audio/video transcription
│   └── util/
│       ├── arweave.py      # Arweave gateway utilities
│       ├── bart_categories.py
│       ├── clickhouse.py   # Database client
│       ├── common.py
│       ├── flair_ner.py    # Flair NER wrapper
│       ├── keybert.py      # KeyBERT wrapper
│       ├── nlp.py          # spaCy utilities
│       └── openai.py       # OpenAI API wrapper
├── sql/
│   ├── arweave/            # Block schema
│   ├── common/             # Utility queries
│   ├── dataos_explore/     # Content schemas
│   └── metas/              # Metadata schemas
├── deployments.py          # Prefect deployment config
├── docker-compose.yml
├── Dockerfile
├── prefect.yaml
├── requirements.txt
├── Pipfile
└── README.md
```

---

## Configuration

### Environment Variables

| Variable | Description |
|----------|-------------|
| `PREFECT_API_URL` | Prefect API endpoint |
| `PREFECT_API_KEY` | Prefect API authentication |
| `CLICKHOUSE_HOST` | ClickHouse server hostname |
| `CLICKHOUSE_PORT` | ClickHouse native port (default: 9000) |
| `CLICKHOUSE_USER` | Database username |
| `CLICKHOUSE_PASSWORD` | Database password |
| `CLICKHOUSE_DB` | Default database name |
| `OPENAI_API_KEY` | OpenAI API key for embeddings |
| `FORCE_GPU` | Force GPU usage (`yes`/`no`) |
| `DEPLOYMENT_NAME` | Prefect deployment identifier |

---

## Technology Stack

- **Python 3.10** - Core runtime
- **Prefect 2.x** - Workflow orchestration
- **ClickHouse** - Analytics database
- **spaCy** - Industrial NLP
- **Flair** - State-of-the-art NER
- **Transformers** - BART classification
- **KeyBERT** - Keyword extraction
- **Faster-Whisper** - Audio transcription
- **OpenAI API** - Text embeddings
- **Docker** - Containerization

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Attribution

Originally developed by **Roark Technology**.

---

<div align="center">



</div>
]]>