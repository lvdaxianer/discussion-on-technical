# Skill Feature Extraction and Matching System Technical Design

> 📖 **中文版本**: [skill-recommend-system.md](./skill-recommend-system.md)

## 1. Problem Background

### 1.1 Business Scenario

Users upload internal skills and external skills to the platform. When other users input their requirements, the system needs to automatically identify and recommend the most relevant skills.

### 1.2 Input Types

| Type | Description |
|------|-------------|
| skill_description | Skill/tool description |
| conversation_context | Conversation context/history |
| user_query | Current user question or requirement |
| document | Technical document or long text |

### 1.3 Core Problems

1. How to extract key features from any type of text input?
2. How to efficiently match user requirements with the Skill library?
3. How to ensure matching accuracy and recall?

---

## 2. Technical Solution Overview

### 2.1 System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          User Input                              │
│     skill description / conversation context / query / document   │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     API Layer (FastAPI)                          │
│  POST /skills/register     - Register Skill                    │
│  POST /skills/recommend   - Recommend Skills                    │
│  GET  /health             - Health Check                       │
└─────────────────────────────┬───────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  LLMChain       │ │  Embeddings     │ │  RetrieverChain │
│  (Feature Extract)│ │  (OpenAI)      │ │  (Multi-way)   │
└────────┬────────┘ └────────┬────────┘ └────────┬────────┘
         │                   │                   │
         └───────────────────┼───────────────────┘
                             ▼
                  ┌─────────────────────┐
                  │   ChromaDB         │
                  │   (Vector Store)   │
                  └─────────────────────┘
```

### 2.2 Tech Stack

| Technology | Purpose | Dependency |
|------------|---------|------------|
| Python 3.10+ | Development Language | System |
| langchain | Core Framework | `langchain>=0.1.0` |
| langchain-openai | OpenAI Integration | `langchain-openai>=0.0.2` |
| langchain-community | Community Integration | `langchain-community>=0.0.2` |
| chromadb | Vector Database | `chromadb>=0.4.0` |
| fastapi | HTTP Service | `fastapi>=0.100.0` |
| jieba | Chinese Tokenization | `jieba>=0.42.1` |

### 2.3 Match Weight Configuration

| Channel | Weight | Description |
|:-------:|:------:|-------------|
| Vector Recall | 0.35 | Embedding Cosine Similarity |
| Intent Match | 0.25 | Keyword Jaccard Similarity |
| Name Match | 0.10 | Title Keyword Hit |
| Domain Match | 0.15 | Domain Tag Match |
| Entity Match | 0.15 | Entity Keyword Match |

---

## 3. Core Module Details

### 3.1 Feature Extraction (LangChain Chain)

#### 3.1.1 Unified Feature Extraction Prompt

```markdown
# Role
You are an information extraction expert that can extract key features from different types of text.

# Input Types
- skill_description: Skill/tool description
- conversation_context: Conversation context/history
- user_query: Current user question or requirement
- document: Technical document or long text

# Features to Extract
1. text_type: Type of input text
2. core_intent: Core intent (1-2 sentences)
3. key_entities: Key entities (comma separated)
4. action_keywords: Action keywords (3-5, comma separated)
5. domain_tags: Domain tags (2-3, comma separated)
6. tech_stack: Tech stack/tools (if any, comma separated)
7. summary: One-sentence summary (max 30 chars)

# Output Format (JSON)
{
  "text_type": "type",
  "core_intent": "intent",
  "key_entities": "entity1, entity2",
  "action_keywords": "action1, action2",
  "domain_tags": "domain1, domain2",
  "tech_stack": "tool1, tool2",
  "summary": "summary"
}
```

#### 3.1.2 LCEL Chain Implementation

```python
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field

class FeatureExtraction(BaseModel):
    text_type: str = Field(description="Input text type")
    core_intent: str = Field(description="Core intent")
    key_entities: str = Field(description="Key entities")
    action_keywords: str = Field(description="Action keywords")
    domain_tags: str = Field(description="Domain tags")
    tech_stack: str = Field(description="Tech stack")
    summary: str = Field(description="One-sentence summary")

# Prompt Template
FEATURE_EXTRACT_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are an information extraction expert..."""),
    ("human", "Input text:\n{input_text}")
])

# Create Chain
chain = (
    {"input_text": RunnablePassthrough()}
    | FEATURE_EXTRACT_PROMPT
    | ChatOpenAI(model="gpt-4o", temperature=0.0)
    | JsonOutputParser(pydantic_object=FeatureExtraction)
)

# Invoke
result = chain.invoke("I want to merge several PDFs into one")
```

### 3.2 Vector Storage (ChromaDB)

#### 3.2.1 Chroma Wrapper

```python
from langchain_community.vectorstores import Chroma
from langchain_openai import OpenAIEmbeddings

# Initialize
vectorstore = Chroma(
    persist_directory="./chroma_data",
    embedding_function=OpenAIEmbeddings(
        model="text-embedding-3-small",
        dimensions=1536
    ),
    collection_name="skills"
)

# Add Texts
vectorstore.add_texts(
    texts=["PDF merge tool description"],
    ids=["skill_001"],
    metadatas=[{
        "skill_id": "skill_001",
        "name": "PDF Merge",
        "core_keywords": "merge,PDF,combine"
    }]
)

# Similarity Search
results = vectorstore.similarity_search_with_score(
    query="I want to merge PDFs",
    k=5
)
```

### 3.3 Multi-way Recall Matching

#### 3.3.1 Recall Flow

```
User Input: "I want to merge several PDFs into one"
                    │
                    ▼
         ┌──────────────────┐
         │  LLM Feature Extract│
         └────────┬─────────┘
                  │
      ┌───────────┼───────────┐
      ▼           ▼           ▼
┌─────────┐ ┌─────────┐ ┌─────────┐
│Vector   │ │Keyword  │ │Name     │
│Recall   │ │Recall   │ │Match    │
│(Chroma) │ │(Jaccard)│ │(Boost)  │
└────┬────┘ └────┬────┘ └────┬────┘
     │           │           │
     └───────────┼───────────┘
                 ▼
          ┌──────────┐
          │  Merge   │
          └──────────┘
                 │
                 ▼
          ┌──────────┐
          │  Sort    │
          └──────────┘
```

#### 3.3.2 Jaccard Similarity

```python
def jaccard_similarity(set1: list, set2: list) -> float:
    """
    Calculate Jaccard Similarity

    Jaccard = |A ∩ B| / |A ∪ B|

    @param set1 Set 1
    @param set2 Set 2
    @returns Similarity [0, 1]
    """
    s1 = {s.lower().strip() for s in set1 if s and s.strip()}
    s2 = {s.lower().strip() for s in set2 if s and s.strip()}

    if not s1 or not s2:
        return 0.0

    intersection = s1 & s2
    union = s1 | s2

    return len(intersection) / len(union) if union else 0.0
```

### 3.4 Stopword Handling

#### 3.4.1 When to Use

| Operation | Remove Stopwords | Reason |
|-----------|-----------------|--------|
| LLM Feature Extract | ❌ No | LLM understands context |
| Embedding | ❌ No | Model handles stopwords |
| Keyword Match | ✅ Yes | Stopwords interfere with overlap |

#### 3.4.2 Implementation

```python
import jieba

STOP_WORDS = {
    'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been',
    'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
    'could', 'should', 'may', 'might', 'can', 'to', 'of', 'in',
    'for', 'on', 'with', 'at', 'by', 'from', 'as', 'into', 'through',
}

def remove_stopwords(tokens: list) -> list:
    """Remove stopwords"""
    return [t for t in tokens if t and t not in STOP_WORDS]

# Usage
text = "I want to merge several PDFs into one"
tokens = jieba.lcut(text)
# ['I', 'want', 'to', 'merge', 'several', 'PDFs', 'into', 'one']

filtered = remove_stopwords(tokens)
# ['want', 'merge', 'PDFs']
```

---

## 4. API Endpoints

### 4.1 Register Skill

```
POST /skills/register
```

**Request Body**:
```json
{
  "skill_id": "skill_001",
  "name": "PDF Merge",
  "description": "Merge multiple PDF files into one, support batch processing"
}
```

**Response**:
```json
{
  "success": true,
  "skill_id": "skill_001",
  "features": {
    "name": "PDF Merge",
    "core_keywords": "merge,PDF,combine",
    "scenarios": "document processing,file operation"
  },
  "message": "Skill registered successfully"
}
```

### 4.2 Recommend Skills

```
POST /skills/recommend
```

**Request Body**:
```json
{
  "query": "I want to merge several PDFs into one",
  "top_k": 5
}
```

**Response**:
```json
{
  "query": "I want to merge several PDFs into one",
  "query_features": {
    "text_type": "user_query",
    "core_intent": "merge multiple PDF files",
    "action_keywords": "merge,combine",
    "domain_tags": "document processing"
  },
  "results": [
    {
      "skill_id": "skill_001",
      "skill_name": "PDF Merge",
      "total_score": 0.897,
      "breakdown": {
        "vector": 0.92,
        "keyword": 0.85,
        "name": 0.8
      }
    }
  ]
}
```

### 4.3 Health Check

```
GET /health
```

**Response**:
```json
{
  "status": "healthy",
  "skill_count": 10,
  "metrics": {
    "recommend_duration_p50": 0.5,
    "recommend_duration_p95": 1.2
  }
}
```

---

## 5. Code Structure

```
skill_recommend/
├── config.py                 # Configuration
├── requirements.txt          # Dependencies
│
├── core/                    # LangChain Core
│   ├── llm.py              # ChatOpenAI Instance
│   ├── embeddings.py        # OpenAIEmbeddings
│   └── prompts.py           # Prompt Templates
│
├── chains/                  # LangChain Chains
│   └── feature_extract_chain.py  # Feature Extract Chain
│
├── vectorstore/             # Vector Storage
│   └── chroma_store.py      # Chroma Wrapper
│
├── matching/                # Matcher
│   ├── matcher.py           # Multi-way Recall
│   └── jaccard.py         # Jaccard Similarity
│
├── api/                     # API Layer
│   ├── routes.py            # Routes
│   └── schemas.py          # Data Models
│
├── utils/
│   └── stopword.py         # Stopword Handling
│
└── main.py                  # Entry Point
```

---

## 6. Deployment Steps

```bash
# 1. Install Dependencies
pip install -r requirements.txt

# 2. Configure Environment
export OPENAI_API_KEY=sk-xxxxx

# 3. Run Service
python main.py

# 4. Test
curl -X POST http://localhost:8000/skills/register \
  -H "Content-Type: application/json" \
  -d '{"skill_id": "001", "name": "PDF Merge", "description": "Merge multiple PDFs into one"}'

curl -X POST http://localhost:8000/skills/recommend \
  -H "Content-Type: application/json" \
  -d '{"query": "I want to merge PDF files"}'
```

---

## 7. Technical Summary

| Module | Key Points |
|--------|------------|
| Feature Extraction | Unified prompt for multi-type input, LangChain LCEL chain |
| Stopwords | Only for keyword matching, not for Embedding/LLM |
| Vector Storage | ChromaDB with persistence |
| Multi-way Recall | Vector + Keyword + Name weighted merge |
| Jaccard | Set overlap calculation, not exact match |

---

## 8. Future Extensions

1. **Cold Start**: Use LLM to analyze user needs when Skill library is empty
2. **Evaluation**: Precision@K, Recall@K, MRR, NDCG@K metrics
3. **Monitoring**: QPS, latency percentiles, click rate
4. **A/B Testing**: Different weight configuration comparison
5. **High Availability**: LLM retry + fallback + multi-key rotation

---

## 9. Dependencies

```txt
# LangChain Core
langchain>=0.1.0
langchain-core>=0.1.0
langchain-openai>=0.0.2
langchain-community>=0.0.2

# Vector Database
chromadb>=0.4.0

# Web Service
fastapi>=0.100.0
uvicorn>=0.23.0
pydantic>=2.0.0

# Utilities
jieba>=0.42.1
python-dotenv>=1.0.0
```

---

> 📝 **Document Version**: 1.0.0
> 📅 **Last Updated**: 2024-01-15
> 👤 **Author**: lvdaxianer
