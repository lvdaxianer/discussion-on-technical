# Skill 特征提取与匹配系统技术方案

> 📖 **English Version**: [skill-recommend-system-en.md](./skill-recommend-system-en.md)

## 一、问题背景

### 1.1 业务场景

用户上传 internal skill（内部技能）和 external skill（外部技能）到平台后，其他用户输入需求时，系统需要能够自动识别并推荐最相关的 skill。

### 1.2 输入类型

| 类型 | 说明 |
|------|------|
| skill_description | 技能/工具描述 |
| conversation_context | 对话上下文/聊天历史 |
| user_query | 用户当前问题或需求 |
| document | 技术文档或长文本 |

### 1.3 核心问题

1. 如何从任意类型的文本输入中提取关键特征？
2. 如何将用户需求与 Skill 库进行高效匹配？
3. 如何保证匹配的准确性和召回率？

---

## 二、技术方案总览

### 2.1 系统架构

```
┌─────────────────────────────────────────────────────────────────┐
│                          用户输入                                  │
│     skill描述 / 对话上下文 / 用户问题 / 技术文档                      │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     API Layer (FastAPI)                          │
│  POST /skills/register     - 注册 Skill                          │
│  POST /skills/recommend   - 推荐 Skill                          │
│  GET  /health             - 健康检查                            │
└─────────────────────────────┬───────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  LLMChain      │ │  Embeddings     │ │  RetrieverChain │
│  (特征提取)     │ │  (OpenAI)      │ │  (多路检索)     │
└────────┬────────┘ └────────┬────────┘ └────────┬────────┘
         │                   │                   │
         └───────────────────┼───────────────────┘
                             ▼
                  ┌─────────────────────┐
                  │   ChromaDB         │
                  │   (向量存储+检索)   │
                  └─────────────────────┘
```

### 2.2 技术栈

| 技术 | 用途 | 依赖 |
|------|------|------|
| Python 3.10+ | 开发语言 | 系统依赖 |
| langchain | 核心框架 | `langchain>=0.1.0` |
| langchain-openai | OpenAI 集成 | `langchain-openai>=0.0.2` |
| langchain-community | 社区集成 | `langchain-community>=0.0.2` |
| chromadb | 向量数据库 | `chromadb>=0.4.0` |
| fastapi | HTTP 服务 | `fastapi>=0.100.0` |
| jieba | 中文分词 | `jieba>=0.42.1` |

### 2.3 匹配权重配置

| 通道 | 权重 | 说明 |
|:----:|:----:|------|
| 向量召回 | 0.35 | Embedding 余弦相似度 |
| 意图匹配 | 0.25 | 关键词 Jaccard 相似度 |
| 名称命中 | 0.10 | 标题关键词命中 |
| 领域匹配 | 0.15 | 领域标签匹配 |
| 实体匹配 | 0.15 | 实体关键词匹配 |

---

## 三、核心模块详解

### 3.1 特征提取（LangChain Chain）

#### 3.1.1 统一特征提取提示词

```markdown
# 角色
你是一个信息抽取专家，能够从不同类型的文本中提取关键特征。

# 输入类型
- skill_description: 技能/工具描述
- conversation_context: 对话上下文/聊天历史
- user_query: 用户当前问题或需求
- document: 技术文档或长文本

# 提取特征
1. text_type: 输入文本的类型
2. core_intent: 核心意图（1-2句话）
3. key_entities: 关键实体（用逗号分隔）
4. action_keywords: 动作关键词（3-5个，用逗号分隔）
5. domain_tags: 领域标签（2-3个，用逗号分隔）
6. tech_stack: 技术栈/工具（如有，用逗号分隔）
7. summary: 一句话总结（不超过30字）

# 输出格式（JSON）
{
  "text_type": "类型",
  "core_intent": "意图",
  "key_entities": "实体1, 实体2",
  "action_keywords": "动作1, 动作2",
  "domain_tags": "领域1, 领域2",
  "tech_stack": "工具1, 工具2",
  "summary": "总结"
}
```

#### 3.1.2 LCEL 链实现

```python
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field

class FeatureExtraction(BaseModel):
    text_type: str = Field(description="输入文本类型")
    core_intent: str = Field(description="核心意图")
    key_entities: str = Field(description="关键实体")
    action_keywords: str = Field(description="动作关键词")
    domain_tags: str = Field(description="领域标签")
    tech_stack: str = Field(description="技术栈")
    summary: str = Field(description="一句话总结")

# 提示词模板
FEATURE_EXTRACT_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """你是一个信息抽取专家..."""),
    ("human", "输入文本：\n{input_text}")
])

# 创建链
chain = (
    {"input_text": RunnablePassthrough()}
    | FEATURE_EXTRACT_PROMPT
    | ChatOpenAI(model="gpt-4o", temperature=0.0)
    | JsonOutputParser(pydantic_object=FeatureExtraction)
)

# 调用
result = chain.invoke("我想要把几个 PDF 合并成一个")
```

### 3.2 向量存储（ChromaDB）

#### 3.2.1 Chroma 封装

```python
from langchain_community.vectorstores import Chroma
from langchain_openai import OpenAIEmbeddings

# 初始化
vectorstore = Chroma(
    persist_directory="./chroma_data",
    embedding_function=OpenAIEmbeddings(
        model="text-embedding-3-small",
        dimensions=1536
    ),
    collection_name="skills"
)

# 添加文本
vectorstore.add_texts(
    texts=["PDF合并工具描述"],
    ids=["skill_001"],
    metadatas=[{
        "skill_id": "skill_001",
        "name": "PDF合并",
        "core_keywords": "合并,PDF,拼接"
    }]
)

# 相似度搜索
results = vectorstore.similarity_search_with_score(
    query="我想要合并PDF",
    k=5
)
```

### 3.3 多路召回匹配

#### 3.3.1 召回流程

```
用户输入: "我想要把几个 PDF 合并成一个"
                    │
                    ▼
         ┌──────────────────┐
         │  LLM 特征提取    │
         └────────┬─────────┘
                  │
      ┌───────────┼───────────┐
      ▼           ▼           ▼
┌─────────┐ ┌─────────┐ ┌─────────┐
│向量召回 │ │关键词召回│ │名称命中 │
│(Chroma)│ │(Jaccard)│ │(Boost) │
└────┬────┘ └────┬────┘ └────┬────┘
     │           │           │
     └───────────┼───────────┘
                 ▼
          ┌──────────┐
          │ 加权合并  │
          └──────────┘
                 │
                 ▼
          ┌──────────┐
          │ 排序输出  │
          └──────────┘
```

#### 3.3.2 Jaccard 相似度

```python
def jaccard_similarity(set1: list, set2: list) -> float:
    """
    计算 Jaccard 相似度

    Jaccard = |A ∩ B| / |A ∪ B|

    @param set1 集合1
    @param set2 集合2
    @returns 相似度 [0, 1]
    """
    s1 = {s.lower().strip() for s in set1 if s and s.strip()}
    s2 = {s.lower().strip() for s in set2 if s and s.strip()}

    if not s1 or not s2:
        return 0.0

    intersection = s1 & s2
    union = s1 | s2

    return len(intersection) / len(union) if union else 0.0
```

#### 3.3.3 多路召回合并

```python
def match(self, query: str, query_features: dict, skills: list, top_k: int = 10) -> list:
    # 1. 向量召回
    vector_results = self._vector_recall(query, top_k * 2)

    # 2. 关键词召回
    keyword_results = self._keyword_recall(query_features, skills, top_k * 2)

    # 3. 合并结果
    merged = self._merge_results(
        vector_results, keyword_results, query_features, skills
    )

    return merged[:top_k]

def _merge_results(self, vector_results, keyword_results, query_features, skills):
    # 归一化 + 加权合并
    weights = {"vector": 0.35, "intent": 0.25, "name": 0.10}

    all_skill_ids = set(vector_results.keys()) | set(keyword_results.keys())

    results = []
    for skill_id in all_skill_ids:
        vector_score = vector_results.get(skill_id, 0)
        keyword_score = keyword_results.get(skill_id, 0)
        name_score = self._compute_name_hit(query_features, skills[skill_id])

        total = (
            weights["vector"] * vector_score +
            weights["intent"] * keyword_score +
            weights["name"] * name_score
        )

        results.append({
            "skill_id": skill_id,
            "score": total
        })

    return sorted(results, key=lambda x: x["score"], reverse=True)
```

### 3.4 停用词处理

#### 3.4.1 何时使用

| 操作 | 是否去停用词 | 原因 |
|------|-------------|------|
| LLM 特征提取 | ❌ 不需要 | LLM 理解上下文语义 |
| Embedding 向量化 | ❌ 不需要 | 模型已处理停用词 |
| 关键词匹配 | ✅ 需要 | 停用词干扰重叠度计算 |

#### 3.4.2 实现

```python
import jieba

STOP_WORDS = {
    '的', '了', '在', '是', '我', '有', '和', '就', '不', '人',
    '都', '一', '一个', '上', '也', '很', '到', '说', '要', '去',
    '你', '会', '着', '没有', '看', '好', '自己', '这', '那', '她',
    '把', '帮', '一下', '想要', '几个', '成', '个', '吧', '呢',
}

def remove_stopwords(tokens: list) -> list:
    """去除停用词"""
    return [t for t in tokens if t and t not in STOP_WORDS]

# 使用示例
text = "我想要把几个 PDF 合并成一个"
tokens = jieba.lcut(text)
# ['我', '想要', '把', '几个', 'PDF', '合并', '成', '一个']

filtered = remove_stopwords(tokens)
# ['想要', 'PDF', '合并', '一个']
```

---

## 四、API 接口

### 4.1 注册 Skill

```
POST /skills/register
```

**请求体**：
```json
{
  "skill_id": "skill_001",
  "name": "PDF合并",
  "description": "将多个PDF文件合并成一个PDF文件，支持批量处理"
}
```

**响应**：
```json
{
  "success": true,
  "skill_id": "skill_001",
  "features": {
    "name": "PDF合并",
    "core_keywords": "合并,PDF,拼接",
    "scenarios": "文档处理,文件操作"
  },
  "message": "Skill 注册成功"
}
```

### 4.2 推荐 Skill

```
POST /skills/recommend
```

**请求体**：
```json
{
  "query": "我想要把几个 PDF 合并成一个",
  "top_k": 5
}
```

**响应**：
```json
{
  "query": "我想要把几个 PDF 合并成一个",
  "query_features": {
    "text_type": "user_query",
    "core_intent": "合并多个PDF文件",
    "action_keywords": "合并,组合",
    "domain_tags": "文档处理"
  },
  "results": [
    {
      "skill_id": "skill_001",
      "skill_name": "PDF合并",
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

### 4.3 健康检查

```
GET /health
```

**响应**：
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

## 五、代码结构

```
skill_recommend/
├── config.py                 # 配置管理
├── requirements.txt          # 依赖清单
│
├── core/                    # LangChain 核心
│   ├── llm.py              # ChatOpenAI 实例
│   ├── embeddings.py        # OpenAIEmbeddings
│   └── prompts.py           # 提示词模板集合
│
├── chains/                  # LangChain Chains
│   └── feature_extract_chain.py  # 特征提取链
│
├── vectorstore/             # 向量存储
│   └── chroma_store.py      # Chroma 封装
│
├── matching/                # 匹配器
│   ├── matcher.py           # 多路召回
│   └── jaccard.py         # Jaccard 相似度
│
├── api/                     # API 层
│   ├── routes.py            # 路由
│   └── schemas.py          # 数据模型
│
├── utils/
│   └── stopword.py         # 停用词处理
│
└── main.py                  # 入口
```

---

## 六、部署步骤

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置环境变量
export OPENAI_API_KEY=sk-xxxxx

# 3. 运行服务
python main.py

# 4. 测试
curl -X POST http://localhost:8000/skills/register \
  -H "Content-Type: application/json" \
  -d '{"skill_id": "001", "name": "PDF合并", "description": "将多个PDF合并成一个"}'

curl -X POST http://localhost:8000/skills/recommend \
  -H "Content-Type: application/json" \
  -d '{"query": "我想要合并PDF文件"}'
```

---

## 七、技术要点总结

| 模块 | 技术要点 |
|------|----------|
| 特征提取 | 统一提示词适配多类型输入，LangChain LCEL 链封装 |
| 停用词 | 只用于关键词匹配通道，Embedding/LLM 不需要 |
| 向量存储 | ChromaDB 存储和检索，支持持久化 |
| 多路召回 | 向量+关键词+名称 三路加权合并 |
| Jaccard | 用于集合重叠度计算，替代精确匹配 |

---

## 八、扩展方向

1. **冷启动优化**：Skill 库为空时，使用 LLM 直接分析用户需求
2. **效果评估**：Precision@K、Recall@K、MRR、NDCG@K 指标
3. **监控指标**：QPS、延迟百分位、点击率
4. **A/B 测试**：不同权重配置的效果对比
5. **高可用**：LLM 重试+降级+多 Key 轮换

---

## 九、依赖清单

```txt
# LangChain 核心
langchain>=0.1.0
langchain-core>=0.1.0
langchain-openai>=0.0.2
langchain-community>=0.0.2

# 向量数据库
chromadb>=0.4.0

# Web 服务
fastapi>=0.100.0
uvicorn>=0.23.0
pydantic>=2.0.0

# 工具
jieba>=0.42.1
python-dotenv>=1.0.0
```

---

> 📝 **Document Version**: 1.0.0
> 📅 **Last Updated**: 2024-01-15
> 👤 **Author**: lvdaxianer
