# Chunked Upload Service

基于 **FastAPI + SQLite** 的可断点续传分片文件上传服务：

- 创建上传会话，按序号接收携带 **SHA-256** 校验的分片
- 分片**乱序上传、失败重试**均可，重复上传幂等
- 全部分片校验通过后才**原子合并**（临时文件 + `rename`）为目标文件
- 完成的文件可直接 **GET 下载**：支持完整下载与单段 `Range`（断点续传），携带以整文件 SHA-256 生成的强 **ETag**
- 查询接口实时返回**缺失分片列表**与会话状态
- 元数据存 SQLite、分片存磁盘，**服务重启后可继续未完成会话**
- 启动时自动初始化存储目录，默认监听 **8000** 端口

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
```

### 本地运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

启动后访问交互式 API 文档：<http://localhost:8000/docs>

### 环境变量

| 变量       | 默认值   | 说明                                   |
| ---------- | -------- | -------------------------------------- |
| `DATA_DIR` | `./data` | 存储根目录（SQLite、分片、成品文件）   |
| `PORT`     | `8000`   | 监听端口（文档用途，以启动参数为准）   |

存储布局（`DATA_DIR` 下）：

```
data/
├── uploads.db          # SQLite：会话与分片元数据（WAL 模式）
├── sessions/<id>/chunks/NNNNNNNN.part   # 已接收分片
└── files/<id>_<filename>                # 合并完成的文件
```

## API 一览

| 方法   | 路径                                  | 说明                     |
| ------ | ------------------------------------- | ------------------------ |
| POST   | `/uploads`                            | 创建上传会话             |
| PUT    | `/uploads/{id}/chunks/{index}`        | 上传一个分片（原始字节） |
| GET    | `/uploads/{id}`                       | 查询状态与缺失分片       |
| POST   | `/uploads/{id}/complete`              | 校验并原子合并           |
| GET    | `/uploads/{id}/content`               | 下载成品文件（支持 Range） |
| GET    | `/uploads`                            | 列出所有会话             |
| DELETE | `/uploads/{id}`                       | 中止会话并清理           |
| GET    | `/healthz`                            | 健康检查                 |

### 1. 创建会话

```bash
curl -X POST http://localhost:8000/uploads \
  -H 'Content-Type: application/json' \
  -d '{
        "filename": "video.mp4",
        "total_size": 2109497,
        "chunk_size": 1048576,
        "file_sha256": "<可选：整文件 SHA-256，合并时校验>"
      }'
```

响应（`201`）中包含 `session_id`、推导出的 `total_chunks` 与初始 `missing_chunks`：

```json
{
  "session_id": "fd865e4c70874dffbf42abbe1bcf9f9d",
  "status": "uploading",
  "total_chunks": 3,
  "chunk_size": 1048576,
  "received_count": 0,
  "missing_chunks": [0, 1, 2]
}
```

### 2. 上传分片（乱序 / 重试均可）

分片以**原始字节**放在请求体中，SHA-256 通过 `X-Chunk-SHA256` 请求头（或 `?sha256=` 查询参数）携带：

```bash
curl -X PUT "http://localhost:8000/uploads/$SID/chunks/2" \
  -H "X-Chunk-SHA256: $(sha256sum part-2.bin | cut -d' ' -f1)" \
  --data-binary @part-2.bin
```

校验规则（任一不满足即返回 `400`，且不落盘，可直接重试）：

- 除最后一片外，每个分片必须恰好为 `chunk_size` 字节；最后一片为剩余字节数
- 请求体的 SHA-256 必须与声明值一致

同一序号重复上传会覆盖旧数据（幂等），网络重试安全。

### 3. 查询进度

```bash
curl http://localhost:8000/uploads/$SID
```

```json
{
  "status": "uploading",
  "received_count": 2,
  "missing_chunks": [1],
  "total_chunks": 3
}
```

### 4. 合并

```bash
curl -X POST http://localhost:8000/uploads/$SID/complete
```

- 分片未齐：返回 `409`，响应体带 `missing_chunks`
- 合并时**逐分片重新校验 SHA-256**，再校验整文件哈希（若创建时提供）
- 目标文件先写入 `*.tmp`，再 `rename` 原子发布；成功响应：

```json
{
  "status": "completed",
  "file_sha256": "e92f5a7f…",
  "final_path": "/data/files/fd865e4c…_video.mp4"
}
```

`complete` 幂等：重复调用返回相同结果。整文件哈希不匹配返回 `422`，会话保持 `uploading`，可修正后重试。

### 5. 下载成品文件（支持 Range / 断点续传）

仅 `completed` 状态的会话可下载；未完成返回 `409`，未知会话返回 `404`。响应始终携带：

- `Accept-Ranges: bytes`
- 强 **ETag**：以整文件 SHA-256 生成，形如 `"e92f5a7f…"`（合并时即固定）
- `Content-Length`；区间下载（`206`）另带 `Content-Range: bytes start-end/总长度`

```bash
# 完整下载（200）
curl -o video.mp4 http://localhost:8000/uploads/$SID/content

# 指定单段区间（206 Partial Content）
curl -H 'Range: bytes=0-1048575' -o part.bin \
  http://localhost:8000/uploads/$SID/content

# 断点续传：curl -C - 会自动补上 Range 请求头
curl -C - -o video.mp4 http://localhost:8000/uploads/$SID/content
```

支持的三种单段写法（仅支持**单段**）：

| Range                  | 含义                                   |
| ---------------------- | -------------------------------------- |
| `bytes=start-end`      | 第 start 到第 end 字节（闭区间，从 0 计） |
| `bytes=start-`        | 从 start 到文件尾                       |
| `bytes=-suffix`       | 文件末尾 suffix 字节                     |

条件请求与错误码：

- `If-None-Match` 命中当前 ETag（弱比较，`W/` 前缀或 `*` 亦可）→ `304 Not Modified`，无响应体：

```bash
curl -H 'If-None-Match: "e92f5a7f…"' -i \
  http://localhost:8000/uploads/$SID/content
```

- `If-Range` 与当前 ETag **不一致**（强比较，弱 ETag 不匹配；HTTP 日期不支持）→ 忽略 `Range`，返回 `200` 完整文件；一致才返回 `206`
- 多段（`bytes=0-1,3-4`）、语法错误或越界范围 → `416 Requested Range Not Satisfiable`，并携带 `Content-Range: bytes */总长度`：

```http
HTTP/1.1 416 Requested Range Not Satisfiable
Content-Range: bytes */3000000
```

下载时按区间在磁盘上**分块流式读取**（1 MiB 缓冲），不会把整个文件载入内存。

## 断点续传语义

- 所有状态都在 `DATA_DIR` 内（SQLite + 分片文件），进程重启、容器重建（挂载同一 volume）后，未完成会话自动可用
- 启动时自动对账：数据库记录与磁盘分片逐一核对，缺失或尺寸不符的记录会被剔除，客户端按 `missing_chunks` 重传即可
- 分片写入同样先写临时文件再原子改名，不存在半写的分片

## 测试

```bash
pip install -r requirements-dev.txt
pytest tests/
```

测试覆盖：乱序上传、错误哈希拒绝与重试、重复分片幂等、未齐时合并返回 409、重启续传、整文件哈希校验、会话中止、完整/区间下载、ETag 与 304、If-Range 回退、416 越界处理。

## 客户端示例（Python）

```python
import hashlib, os, requests

BASE = "http://localhost:8000"
path, chunk_size = "video.mp4", 1024 * 1024
size = os.path.getsize(path)
sha = hashlib.sha256(open(path, "rb").read()).hexdigest()

sid = requests.post(f"{BASE}/uploads", json={
    "filename": os.path.basename(path),
    "total_size": size,
    "chunk_size": chunk_size,
    "file_sha256": sha,
}).json()["session_id"]

with open(path, "rb") as f:
    for index in range((size + chunk_size - 1) // chunk_size):
        data = f.read(chunk_size)
        digest = hashlib.sha256(data).hexdigest()
        r = requests.put(f"{BASE}/uploads/{sid}/chunks/{index}",
                         data=data, headers={"X-Chunk-SHA256": digest})
        r.raise_for_status()

print(requests.post(f"{BASE}/uploads/{sid}/complete").json())

# 下载：完整 / 区间（stream=True 分块落盘，不占大内存）
with requests.get(f"{BASE}/uploads/{sid}/content", stream=True) as r:
    r.raise_for_status()
    with open("downloaded.mp4", "wb") as out:
        for block in r.iter_content(1024 * 1024):
            out.write(block)

# 断点续传：从第 1 MiB 开始取剩余部分
etag = '"e92f5a7f…"'  # 上次响应里的 ETag
r = requests.get(
    f"{BASE}/uploads/{sid}/content",
    headers={"Range": "bytes=1048576-", "If-Range": etag},
)
assert r.status_code in (200, 206)  # 206=区间有效；200=文件已变，给的是完整内容
```
