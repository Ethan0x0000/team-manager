# 外部 API 对接文档 — Team 账号自动导入

> 本文档面向需要开发**本地服务**，通过 API 向本系统自动上传 Team 账号的开发者。

---

## 目录

1. [前置准备](#1-前置准备)
2. [认证方式](#2-认证方式)
3. [接口列表](#3-接口列表)
   - [3.1 单账号导入](#31-单账号导入)
   - [3.2 批量导入](#32-批量导入)
   - [3.3 JSON 批量导入](#33-json-批量导入)
4. [Webhook 回调（可选）](#4-webhook-回调可选)
5. [完整示例代码](#5-完整示例代码)
6. [错误码与排错](#6-错误码与排错)
7. [注意事项](#7-注意事项)

---

## 1. 前置准备

### 1.1 获取 API Key

1. 登录管理后台 → **系统设置** → **库存预警** 标签页
2. 在 **API Key (用于补货对接)** 字段中填入一个足够复杂的随机字符串
3. 点击 **保存配置**

> 此 API Key 等同于管理员身份凭证，请妥善保管，不要泄露。

### 1.2 确认系统地址

假设你的管理系统部署在 `http://your-server:8008`，以下所有接口的完整 URL 均以此为前缀。

---

## 2. 认证方式

所有管理接口通过 HTTP Header `X-API-Key` 进行认证：

```
X-API-Key: 你在系统设置中配置的API密钥
```

认证失败返回：

```json
HTTP 401
{
    "detail": "未登录或 API Key 无效"
}
```

---

## 3. 接口列表

### 3.1 单账号导入

逐个上传 Team 账号，适合实时补货场景。

**请求**

```
POST /admin/teams/import
Content-Type: application/json
X-API-Key: <your-api-key>
```

**请求体**

```json
{
    "import_type": "single",
    "access_token": "eyJhbGci...",
    "email": "team@example.com",
    "account_id": "org-xxxx",
    "refresh_token": "rt-xxxx",
    "session_token": "sess-xxxx",
    "client_id": "app_xxxx",
    "id_token": "eyJhbGci...",
    "pool_type": "normal"
}
```

**字段说明**

| 字段 | 类型 | 必填 | 说明 |
|:---|:---|:---|:---|
| `import_type` | string | **是** | 固定为 `"single"` |
| `access_token` | string | 建议 | ChatGPT Access Token (AT)。三种 Token 至少提供一种 |
| `session_token` | string | 建议 | Session Token (ST)。如果 AT 缺失/过期，系统会用 ST 自动刷新获取 AT |
| `refresh_token` | string | 可选 | Refresh Token (RT)。需配合 `client_id` 使用 |
| `client_id` | string | 可选 | OAuth Client ID，配合 RT 刷新 AT 使用。若不填，系统会尝试从 AT 中解析或使用全局默认值 |
| `id_token` | string | 可选 | ID Token，若不填系统会自动尝试获取 |
| `email` | string | 可选 | 账号邮箱。若不填，系统从 AT 中自动解析 |
| `account_id` | string | 可选 | Team 的 Account ID。若不填，系统自动获取该账号下所有活跃 Team 并逐一导入 |
| `pool_type` | string | 可选 | `"normal"`（默认）或 `"welfare"`（福利池） |

**Token 优先级**：`access_token` > `session_token` > `refresh_token + client_id`

系统会自动尝试以下流程：
1. 如果 `access_token` 有效 → 直接使用
2. 如果 AT 缺失/过期 + 提供了 `session_token` → 用 ST 刷新获取 AT
3. 如果以上都不行 + 提供了 `refresh_token` + `client_id` → 用 RT 刷新获取 AT

**成功响应** `200 OK`

```json
{
    "success": true,
    "team_id": 42,
    "email": "team@example.com",
    "message": "成功导入 1 个 Team 账号",
    "error": null
}
```

如果一个邮箱下有多个活跃 Team（未指定 `account_id`）：

```json
{
    "success": true,
    "team_id": 42,
    "email": "team@example.com",
    "message": "成功导入 3 个 Team 账号 (另有 1 个已存在)",
    "error": null
}
```

**失败响应** `400 Bad Request`

```json
{
    "success": false,
    "team_id": null,
    "email": "team@example.com",
    "message": null,
    "error": "缺少有效的 Access Token，且无法通过 Session/Refresh Token 刷新"
}
```

常见失败原因：
- 未提供任何有效 Token
- Token 过期且无法刷新
- 提供的邮箱与 Token 中的邮箱不匹配
- 所有 Team 账号均已在系统中

---

### 3.2 批量导入

通过纯文本一次性导入多个账号，系统会自动解析文本内容。适合从表格/文件批量迁移。

**请求**

```
POST /admin/teams/import
Content-Type: application/json
X-API-Key: <your-api-key>
```

**请求体**

```json
{
    "import_type": "batch",
    "content": "email1@example.com,eyJhbGci...,rt-xxxx,,\nemail2@example.com,eyJhbGci...,,,",
    "pool_type": "normal"
}
```

**字段说明**

| 字段 | 类型 | 必填 | 说明 |
|:---|:---|:---|:---|
| `import_type` | string | **是** | 固定为 `"batch"` |
| `content` | string | **是** | 包含账号信息的文本，多条用换行分隔 |
| `pool_type` | string | 可选 | `"normal"` 或 `"welfare"` |

**`content` 文本格式**

每行一个账号，字段用逗号分隔：

```
邮箱,Access_Token,Refresh_Token,Session_Token,Client_ID
```

缺失字段用空占位，例如只有 AT：
```
email@example.com,eyJhbGci...,,
```

也支持 `----` 分隔符分隔多条记录。

**响应格式：流式 (NDJSON)**

> ⚠️ 批量导入返回的是 **流式响应**（`application/x-ndjson`），每行一个 JSON 对象。

```jsonl
{"type":"progress","current":1,"total":3,"success":true,"message":"导入成功"}
{"type":"progress","current":2,"total":3,"success":false,"error":"Token 已过期"}
{"type":"progress","current":3,"total":3,"success":true,"message":"导入成功"}
{"type":"finish","total":3,"success_count":2,"failed_count":1}
```

你的本地服务需要逐行读取并解析 JSON。

---

### 3.3 JSON 批量导入

以 JSON 数组格式批量导入，格式更规范。

**请求体**

```json
{
    "import_type": "json",
    "content": "[{\"email\":\"a@example.com\",\"access_token\":\"eyJ...\"},{\"email\":\"b@example.com\",\"access_token\":\"eyJ...\"}]",
    "pool_type": "normal"
}
```

| 字段 | 类型 | 必填 | 说明 |
|:---|:---|:---|:---|
| `import_type` | string | **是** | 固定为 `"json"` |
| `content` | string | **是** | JSON 数组的字符串形式（stringify 后传入） |
| `pool_type` | string | 可选 | `"normal"` 或 `"welfare"` |

**响应格式**：同批量导入，流式 NDJSON。

---

## 4. Webhook 回调（可选）

如果你希望在库存不足时自动触发补货流程，可以配合库存预警 Webhook 使用。

### 4.1 配置方式

在管理后台 → **系统设置** → **库存预警**：

| 配置项 | 说明 |
|:---|:---|
| **Webhook URL** | 你的本地服务接收通知的端点地址 |
| **车位预警阈值** | 全系统总剩余车位 ≤ 此值时触发通知 |
| **API Key** | 同时用于通知请求的 Header 和回调导入的认证 |

### 4.2 通知格式

系统会向你的 Webhook URL 发送 POST 请求：

```
POST <你的 Webhook URL>
Content-Type: application/json
X-API-Key: <配置的 API Key>
```

```json
{
    "event": "low_stock",
    "current_seats": 5,
    "threshold": 10,
    "message": "库存不足预警：系统总可用车位仅剩 5，已低于预警阈值 10，请及时补货导入新账号。"
}
```

**字段说明**

| 字段 | 类型 | 说明 |
|:---|:---|:---|
| `event` | string | 固定为 `"low_stock"` |
| `current_seats` | int | 当前系统总可用车位数 |
| `threshold` | int | 配置的预警阈值 |
| `message` | string | 人类可读的预警描述 |

### 4.3 完整闭环流程

```
┌────────────────────┐   定时检查: 车位(5) ≤ 阈值(10)
│   GPT Team 管理系统  │──────────────────────────────────────┐
│                    │                                       │
│  POST /admin/      │                                       ▼
│  teams/import      │◄────────── 上传新账号 ──────── ┌──────────────┐
│  (X-API-Key认证)    │                               │  你的本地服务  │
└────────────────────┘                               │              │
                                                     │  1. 收到通知   │
                                                     │  2. 获取新账号  │
                                                     │  3. 调导入接口  │
                                                     └──────────────┘
```

---

## 5. 完整示例代码

### 5.1 Python — 主动上传单个账号

```python
import httpx

BASE_URL = "http://your-server:8008"
API_KEY = "你在系统设置中配置的密钥"


def import_single_team(access_token: str, email: str = None):
    """上传单个 Team 账号"""
    payload = {
        "import_type": "single",
        "access_token": access_token,
    }
    if email:
        payload["email"] = email

    response = httpx.post(
        f"{BASE_URL}/admin/teams/import",
        json=payload,
        headers={"X-API-Key": API_KEY},
        timeout=30.0,
    )

    result = response.json()
    if result.get("success"):
        print(f"✅ 导入成功: team_id={result['team_id']}, {result['message']}")
    else:
        print(f"❌ 导入失败: {result.get('error')}")

    return result


# 使用示例
import_single_team("eyJhbGciOiJSUz...", email="team@example.com")
```

### 5.2 Python — 主动批量上传

```python
import httpx


def import_batch_teams(accounts: list[dict]):
    """
    批量上传 Team 账号

    accounts: [{"email": "...", "access_token": "...", "refresh_token": "...", ...}, ...]
    """
    # 构建 CSV 格式文本
    lines = []
    for acc in accounts:
        line = ",".join([
            acc.get("email", ""),
            acc.get("access_token", ""),
            acc.get("refresh_token", ""),
            acc.get("session_token", ""),
            acc.get("client_id", ""),
        ])
        lines.append(line)

    payload = {
        "import_type": "batch",
        "content": "\n".join(lines),
    }

    # 流式读取响应
    with httpx.stream(
        "POST",
        f"{BASE_URL}/admin/teams/import",
        json=payload,
        headers={"X-API-Key": API_KEY},
        timeout=120.0,
    ) as response:
        for line in response.iter_lines():
            if line.strip():
                import json
                data = json.loads(line)
                print(data)


# 使用示例
import_batch_teams([
    {"email": "a@example.com", "access_token": "eyJ..."},
    {"email": "b@example.com", "access_token": "eyJ...", "refresh_token": "rt-..."},
])
```

### 5.3 Python — Webhook 接收 + 自动补货回调

```python
import httpx
from fastapi import FastAPI, Request

app = FastAPI()

BASE_URL = "http://your-server:8008"
API_KEY = "你在系统设置中配置的密钥"


@app.post("/webhook/low-stock")
async def handle_low_stock_notification(request: Request):
    """接收库存预警通知并自动补货"""
    data = await request.json()

    # 1. 验证来源（可选：校验 X-API-Key Header）
    incoming_key = request.headers.get("X-API-Key")
    if incoming_key != API_KEY:
        return {"status": "rejected", "reason": "invalid api key"}

    print(f"📦 收到库存预警: 剩余车位 {data['current_seats']}, 阈值 {data['threshold']}")

    # 2. 从你自己的货源获取新账号（此处为示例）
    new_accounts = get_new_accounts_from_your_source()

    # 3. 逐个调用导入接口
    for account in new_accounts:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{BASE_URL}/admin/teams/import",
                json={
                    "import_type": "single",
                    "access_token": account["access_token"],
                    "email": account.get("email"),
                },
                headers={"X-API-Key": API_KEY},
            )
            result = response.json()
            print(f"  导入结果: {result}")

    return {"status": "ok", "imported": len(new_accounts)}


def get_new_accounts_from_your_source() -> list[dict]:
    """替换为你自己的账号获取逻辑"""
    return [
        {"email": "new_team@example.com", "access_token": "eyJ..."},
    ]
```

### 5.4 cURL — 快速测试

```bash
# 单账号导入
curl -X POST http://your-server:8008/admin/teams/import \
  -H "Content-Type: application/json" \
  -H "X-API-Key: 你的密钥" \
  -d '{
    "import_type": "single",
    "access_token": "eyJhbGciOi...",
    "email": "team@example.com"
  }'

# 预期返回
# {"success":true,"team_id":42,"email":"team@example.com","message":"成功导入 1 个 Team 账号","error":null}
```

---

## 6. 错误码与排错

### HTTP 状态码

| 状态码 | 含义 | 处理方式 |
|:---|:---|:---|
| `200` | 请求成功（检查 `success` 字段确认业务结果） | — |
| `400` | 请求参数错误或业务校验失败 | 检查 `error` 字段 |
| `401` | API Key 无效或未提供 | 检查 Header 中的 `X-API-Key` 是否与系统设置一致 |
| `500` | 服务器内部错误 | 查看管理系统日志排查 |

### 常见业务错误

| error 信息 | 原因 | 解决方式 |
|:---|:---|:---|
| `必须提供 Access Token、Refresh Token 或 Session Token 其中之一` | 三种 Token 都没提供 | 至少提供一种有效 Token |
| `缺少有效的 Access Token，且无法通过 Session/Refresh Token 刷新` | AT 过期且 ST/RT 刷新都失败 | 提供新的有效 Token |
| `Token 对应的账号身份 (a@x.com) 与提供的邮箱 (b@x.com) 不符` | email 字段和 Token 中解析的邮箱不一致 | 修正 email 或不传（让系统自动解析） |
| `共发现 N 个 Team 账号,但均已在系统中` | 重复导入 | 正常情况，跳过即可 |
| `该 Token 没有关联任何 Team 账户` | Token 对应的 OpenAI 账号没有 Team | 检查账号是否确实有 Team 订阅 |
| `无法从 Token 中提取邮箱,请手动提供邮箱` | AT 中没有 email claim | 在请求中添加 `email` 字段 |

---

## 7. 注意事项

1. **API Key 安全性**：此 Key 等同于管理员权限，能访问所有管理接口（不只是导入），务必只在可信环境使用
2. **Token 有效期**：Access Token 通常有效期较短（~1 小时），建议优先提供 `session_token` 或 `refresh_token`，系统会自动刷新
3. **重复导入**：系统按 `account_id` + `pool_type` 去重，重复导入不会创建重复记录，会返回"已存在"提示
4. **不指定 `account_id`**：如果一个邮箱下有多个活跃 Team，系统会自动发现并全部导入
5. **批量导入是流式响应**：客户端需要逐行读取 NDJSON，不能直接 `response.json()` 解析
6. **网络超时**：单账号导入涉及调用 OpenAI API 获取 Team 信息，建议设置 30s 以上超时
7. **代理配置**：如果管理系统需要通过代理访问 OpenAI，确保系统设置中代理已正确配置，导入时系统会自动使用
