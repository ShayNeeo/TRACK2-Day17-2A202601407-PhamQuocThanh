# Báo cáo Nghiệm Thu Kỹ Thuật — LAB 17: Data Pipeline Engineering

**Kỹ sư thực hiện:** Phạm Quốc Thanh  
**Mã sinh viên:** 2A202601407  
**Lớp:** AICB-P2T2  
**Hệ thống:** Customer Support AI Platform Data Lakehouse (DuckDB + dbt + Airflow)  
**Ngày nghiệm thu:** 2026-08-17  

---

## 0 · Bảng Tổng Hợp Kết Quả Nghiệm Thu (`make verify`)

Hệ thống đã vượt qua toàn bộ **4/4 tiêu chí cốt lõi** và **2/2 bài toán tối ưu hóa mở rộng (Extra A & B)** với điểm số tối đa **110 / 100 điểm**.

| Hạng mục kiểm tra | Trạng thái | Số hàng thực tế | Kỳ vọng chuẩn | Băm Checksum (3 lượt) | Đánh giá |
|---|:---:|:---:|:---:|:---:|:---:|
| `gold_training_set` (Idempotent) | ỔN ĐỊNH ✓ | 12,480 | 12,480 | `8dd7c98653` | ĐẠT ✓ |
| `gold_feature_daily` (Late-arriving) | ỔN ĐỊNH ✓ | 9,100 | 9,100 | `3db448685c` | ĐẠT ✓ |
| `gold_doc_chunks` (Đối chứng RAG) | ỔN ĐỊNH ✓ | 31,200 | 31,200 | `92d8e50131` | ĐẠT ✓ |
| `quarantine_tickets` (Cách ly lỗi) | ỔN ĐỊNH ✓ | 312 | 312 | `ebb89036fb` | ĐẠT ✓ |
| `silver_tickets.priority` (Data Contract) | SẠCH ✓ | 12,480 | 12,480 | 0 NULL, 100% ∈ 1..4 | ĐẠT ✓ |
| `dbt test` (Bộ kiểm thử ràng buộc) | PASS ✓ | 11/11 tests | ≥ 9 tests | Thời gian: 0.33s | ĐẠT ✓ |
| `dashboard rows scanned` (Bài mở rộng A) | TỐI ƯU ✓ | 140,126 | ≤ 500,000 | Giảm **35.7×** (5.0M → 140k) | ĐẠT ✓ |
| `consumer crash tolerance` (Bài mở rộng B) | BẢO TOÀN ✓ | 20,000 | 20,000 | Lost = 0, Dup = 0 (C == A) | ĐẠT ✓ |
| `Airflow DAG Safety Config` | AN TOÀN ✓ | catchup=False | max_active=1 | AST Verified | ĐẠT ✓ |

<details open>
<summary><b>📄 Chi tiết Terminal Output: <code>make verify</code> (Nghiệm thu 3 chu kỳ liên tiếp)</b></summary>

```text
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 27.9s
  run 2/3 … 27.6s
  run 3/3 … 27.9s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 140,126 (35.7×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

---

## 1 · Nhiệm vụ 1: Khắc Phục Bùng Nổ Dữ Liệu Khi Rerun Pipeline (Incident #1041)

### 1.1. Triệu chứng & Hiện trường sự cố
Khi tiến hành thao tác **Clear Task** trên giao diện Airflow hoặc khi pipeline chạy lại một partition ngày, kích thước bảng `gold_training_set` tăng vọt từ 12,480 hàng ban đầu lên 25,620 hàng (sau lượt 2) và đạt 38,750 hàng (sau lượt 3). Các ticket bị nhân bản hàng loạt, khiến mô hình AI Classifier học trên tập dữ liệu trùng lặp và làm lệch trọng số dự đoán.

### 1.2. Phân tích nguyên nhân gốc rễ (Root Cause Analysis)
1. **Thiếu chiến lược Idempotency ở dbt model:** File `dbt/models/gold/gold_training_set.sql` được cấu hình `materialized='incremental'` nhưng không khai báo `unique_key` và `incremental_strategy`. Trong dbt, khi thiếu `unique_key`, hành vi mặc định của bộ nạp là sinh câu lệnh `INSERT INTO target SELECT ...` (Append-only). Cứ mỗi lần chạy lại, toàn bộ dữ liệu của ngày đó được chèn chồng lên bảng đích.
2. **Cân nhắc thiết kế (Tại sao không dùng xóa theo partition ngày `delete+insert` theo ngày?):**
   - Dữ liệu nguồn là **CDC (Change Data Capture)** từ Postgres, chứa các thao tác cập nhật (`op = 'u'`). Một ticket phát sinh vào ngày D1 nhưng được cập nhật nội dung/trạng thái ở các ngày D2, D3.
   - Nếu dùng chiến lược xóa partition theo ngày phát sinh event, khi ngày D2 chạy lại, bản ghi ngày D1 của cùng ticket đó vẫn còn tồn tại trong kho $\to$ Ticket vẫn bị nhân bản theo trục thời gian.
   - **Quyết định đúng đắn:** Phải định nghĩa thực thể theo định danh nghiệp vụ `unique_key = 'ticket_id'` kết hợp chiến lược `incremental_strategy = 'delete+insert'`.
3. **Cấu hình nguy hiểm trên Airflow DAG:** File `dags/ai_training_pipeline.py` để `catchup = True` và không giới hạn `max_active_runs`. Khi Clear Task hoặc khi dồn lịch chạy, Airflow kích hoạt nhiều DAG run đồng thời, gây tranh chấp ghi đè và dồn ép dữ liệu.

### 1.3. Bằng chứng đối chiếu kỹ thuật (Evidence)

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ SO SÁNH TRƯỚC VÀ SAU KHI KHẮC PHỤC SỰ CỐ #1041 (GOLD_TRAINING_SET)                     │
├──────────────────────────┬─────────────────────────────┬───────────────────────────────┤
│ Chỉ số đo lường          │ Trước khi sửa (Buggy)       │ Sau khi sửa (Idempotent)      │
├──────────────────────────┼─────────────────────────────┼───────────────────────────────┤
│ Số hàng sau Run 1        │ 12,480 hàng                 │ 12,480 hàng                   │
│ Số hàng sau Run 2        │ 25,620 hàng (+13,140 hàng)  │ 12,480 hàng (Ghi đè chuẩn xác)│
│ Số hàng sau Run 3        │ 38,750 hàng (+13,130 hàng)  │ 12,480 hàng (Bất biến)        │
│ Số lượng Distinct Ticket │ 12,480 ticket               │ 12,480 ticket                 │
│ Tỷ lệ trùng lặp (Dup)    │ 210.5% (26,270 hàng lặp)    │ 0.0% (1 hàng / 1 ticket)      │
│ Checksum Hash (3 lượt)   │ Bị lệch sau mỗi lần chạy    │ 8dd7c98653 (Tuyệt đối ổn định)│
└──────────────────────────┴─────────────────────────────┴───────────────────────────────┘
```

<details>
<summary><b>🔍 Trích đoạn Code Diff & Kiểm tra DAG AST</b></summary>

**1. Thay đổi cấu hình tại `dbt/models/gold/gold_training_set.sql`:**
```diff
 {{ config(
-    materialized     = 'incremental',
-    on_schema_change = 'fail'
+    materialized         = 'incremental',
+    unique_key           = 'ticket_id',
+    incremental_strategy = 'delete+insert',
+    on_schema_change     = 'fail'
 ) }}
```

**2. Thay đổi tham số tại `dags/ai_training_pipeline.py`:**
```diff
 with DAG(
     "ai_training_pipeline",
     default_args=default_args,
     schedule_interval="@daily",
-    catchup=True,
-    # max_active_runs=?
+    catchup=False,
+    max_active_runs=1,
 ) as dag:
```

**3. Kết quả kiểm tra AST (`tools/check_dag.py`):**
```json
{"catchup": false, "max_active_runs": 1, "ok": true}
```

</details>

---

## 2 · Nhiệm vụ 2: Thu Hồi Dữ Liệu Về Muộn (Late-Arriving Events — Incident #1043)

### 2.1. Triệu chứng & Hiện trường sự cố
Bảng đặc trưng theo ngày `gold_feature_daily` (phục vụ Routing Agent) bị thiếu hụt ~5% số hàng so với đối chiếu thực tế (chỉ đạt 8,645 trên tổng số 9,100 cặp ngày-khách hàng). Hiện tượng đặc biệt: Các ngày mới chạy thì đủ dữ liệu, nhưng các ngày trong quá khứ bị hụt dần theo thời gian.

### 2.2. Đo lường thực nghiệm phân bố độ trễ (Empirical Latency Profiling)
Thay vì phán đoán cảm tính, chúng tôi tiến hành truy vấn trực tiếp phân tích độ trễ giữa thời điểm phát sinh sự kiện (`event_time`) và thời điểm sự kiện nạp vào kho (`_ingested_at`) trên toàn bộ 130,683 bản ghi `bronze_events`.

<details open>
<summary><b>📊 SQL Query & Kết quả đo lường phân bố độ trễ</b></summary>

```sql
select
    count(*) as total_events,
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.50) as p50_days,
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.95) as p95_days,
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99) as p99_days,
    max(date_diff('second', event_time, _ingested_at)/86400.0) as max_days,
    count(case when date_diff('second', event_time, _ingested_at) > 86400 then 1 end)*100.0 / count(*) as late_ratio_pct
from bronze_events;
```

**Bảng số liệu thực nghiệm thu được:**
- **Tổng số sự kiện:** 130,683 events
- **Phân vị P50 (Trung vị):** `0.128 ngày` (~3.07 giờ)
- **Phân vị P95:** `1.814 ngày` (~43.5 giờ)
- **Phân vị P99:** `2.726 ngày` (~65.4 giờ)
- **Độ trễ lớn nhất (Max):** `2.945 ngày` (~70.7 giờ)
- **Tỷ lệ sự kiện đến muộn (> 1 ngày):** `5.05%` *(Khớp chính xác với tỷ lệ 5% hàng bị hụt)*

</details>

### 2.3. Cân nhắc kỹ thuật: Vì sao chọn P99 làm căn cứ thay vì `max`?
> **Phân tích Đánh đổi (Trade-off Analysis):**
> 1. **Bản chất của P99:** P99 phản ánh ngưỡng giới hạn bao phủ 99% hành vi mạng thực tế, loại bỏ hoàn toàn các điểm ngoại lai bất thường (outliers do lỗi xung đột đồng hồ client, buffer kẹt). 
> 2. **Chi phí tính toán (Compute Overhead):** Mỗi khi mở rộng Lookback Window thêm 1 ngày, ở **tất cả** các chu kỳ chạy hàng ngày tiếp theo, dbt đều phải quét lại toàn bộ dữ liệu, tính toán lại aggregation và thực hiện `DELETE + INSERT` cho toàn bộ số ngày đó. Nếu chọn `max` trong production (nơi có event trễ 30-90 ngày do client offline), chi phí compute sẽ tăng gấp hàng chục lần mà không đem lại giá trị nghiệp vụ đáng kể.
> 3. **Quyết định:** Với dữ liệu thực tế $P99 = 2.73\text{ ngày}$ và $Max = 2.94\text{ ngày}$ nằm sát nhau dưới ngưỡng 3 ngày, việc thiết lập **Lookback 3 ngày** kết hợp khóa kết hợp `unique_key = ['event_date', 'customer_id']` là phương án tối ưu: bao phủ 100% dữ liệu muộn với chi phí tối thiểu.

### 2.4. Bằng chứng đối chiếu kỹ thuật (Evidence)

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ SO SÁNH TRƯỚC VÀ SAU KHI XỬ LÝ LATE-ARRIVING DATA (GOLD_FEATURE_DAILY)                 │
├──────────────────────────┬─────────────────────────────┬───────────────────────────────┤
│ Tiêu chí đánh giá        │ Trước khi sửa (No Lookback) │ Sau khi sửa (Lookback 3 days) │
├──────────────────────────┼─────────────────────────────┼───────────────────────────────┤
│ Mệnh đề lọc thời gian    │ event_date > max(event_date)│ event_date >= max - 3 days    │
│ Số hàng thu được         │ 8,645 hàng                  │ 9,100 hàng (100% đầy đủ)      │
│ Số ngày bao phủ          │ 14 ngày (bị rỗng một phần)  │ Đủ 14 ngày × 650 customer     │
│ Hàng bị mất mát          │ 455 hàng (5.00%)            │ 0 hàng (0.00%)                │
│ Checksum Hash (3 lượt)   │ Không ổn định               │ 3db448685c (Nhất quán)        │
└──────────────────────────┴─────────────────────────────┴───────────────────────────────┘
```

<details>
<summary><b>🔍 Trích đoạn Code Diff tại <code>dbt/models/gold/gold_feature_daily.sql</code></b></summary>

```diff
 {{ config(
-    materialized     = 'incremental',
-    on_schema_change = 'fail'
+    materialized         = 'incremental',
+    unique_key           = ['event_date', 'customer_id'],
+    incremental_strategy = 'delete+insert',
+    on_schema_change     = 'fail'
 ) }}
 
 select
     event_date,
     customer_id,
     customer_name,
     count(*)                                        as total_events,
     round(avg(latency_ms), 2)                       as avg_latency_ms,
     quantile_cont(latency_ms, 0.95)::int            as p95_latency_ms,
     sum(case when is_escalated then 1 else 0 end)   as n_escalated,
     sum(tokens_in + tokens_out)                     as tokens_total
 from {{ ref('silver_events') }}
 
 {% if is_incremental() %}
-where event_date > (select max(event_date) from {{ this }})
+where event_date >= (select max(event_date) from {{ this }}) - interval 3 day
 {% endif %}
 
 group by 1, 2, 3, 4
```

</details>

---

## 3 · Nhiệm vụ 3: Data Contracts, Schema Evolution & Cách Ly Lỗi (Incident #1047)

### 3.1. Triệu chứng & Hiện trường sự cố
Team Backend nâng cấp hệ thống và đổi kiểu cột `priority` sang chuỗi (`'urgent'`, `'high'`, `'medium'`, `'low'`), đồng thời để lọt các dữ liệu lỗi/rác (`'0'`, `'5'`, `'-1'`, `'unknown'`, `'P1'`, `''`, `NULL`). 
Hàm `try_cast(priority_raw as integer)` cũ của pipeline dẫn tới hậu quả kép:
1. Ép toàn bộ các nhãn chuỗi hợp lệ thành `NULL` $\to$ Đánh mất hơn 6,600 bản ghi sạch.
2. Cho lọt các số nguyên ngoài khoảng như `0`, `5`, `-1` vào tầng Silver $\to$ Làm suy giảm nghiêm trọng độ chính xác của AI Classifier.
3. Bảng `quarantine_tickets` chứa điều kiện giả lập `where false` nên rỗng hoàn toàn (0 / 312 hàng).

### 3.2. Cấu trúc 3 nhóm giá trị và quy tắc xử lý
Chúng tôi thiết lập ma trận chuẩn hoá phân định rõ giữa **Schema Evolution** (biến đổi định dạng hợp pháp) và **Corrupt Data** (dữ liệu sai lệch cần cách ly):

| Nhóm dữ liệu | Ví dụ giá trị đầu vào | Bản chất kỹ thuật | Xử lý tại `normalize_priority` | Đích đến của bản ghi |
|---|---|---|---|---|
| **Nhóm 1: Số nguyên hợp lệ** | `'1'`, `'2'`, `'3'`, `'4'` | Dữ liệu cũ chuẩn | Ép kiểu `integer` (1..4) | `silver_tickets` |
| **Nhóm 2: Schema Evolution** | `'urgent'`, `'high'`, `'medium'`, `'low'` | Chuẩn mới của Backend | Map tương ứng: `urgent→1, high→2, medium→3, low→4` | `silver_tickets` |
| **Nhóm 3: Dữ liệu lỗi (Corrupt)** | `'0'`, `'5'`, `'-1'`, `'P1'`, `'unknown'`, `''`, `NULL` | Vi phạm Contract | Trả về `NULL` | `quarantine_tickets` (312 hàng) |

### 3.3. Câu hỏi thiết kế kiến trúc:
> **1. Vì sao nên chặn và cách ly ở tầng Silver thay vì Bronze?**
> * Tầng Bronze đóng vai trò là **Raw Landing Zone / Immutable Audit Log**. Nhiệm vụ tối thượng của Bronze là ghi nhận trung thực 100% dữ liệu phát sinh từ nguồn (kể cả đúng lẫn sai) để phục vụ việc kiểm toán (compliance), truy vết lỗi và replay dữ liệu khi backend phát hành bản vá. Nếu chặn ở Bronze, dữ liệu bị vứt bỏ vĩnh viễn và không thể điều tra nguyên nhân.
>
> **2. Vì sao KHÔNG để pipeline dừng lại (Fail-Fast toàn bộ DAG) khi gặp bản ghi lỗi?**
> * Trong 14,300 bản ghi CDC, chỉ có đúng **312 bản ghi bị lỗi (chiếm ~2.1%)**. Nếu áp dụng cơ chế Fail-Fast làm dừng toàn bộ DAG, hơn 97% dữ liệu sạch của ngày và toàn bộ các tác vụ AI downstream (tạo vector RAG, cập nhật bảng Dashboard, tính Feature) sẽ bị đình trệ, vi phạm cam kết chất lượng dịch vụ (SLA).
> * Mô hình **Quarantine Pattern (Dead Letter Queue)** giúp cô lập các bản ghi lỗi vào hàng đợi riêng để kỹ sư xử lý sau, trong khi luồng dữ liệu chính vẫn tiếp tục vận hành với độ sẵn sàng cao nhất (**High Availability**).
>
> **3. Vị trí lọc bản ghi lỗi trong câu lệnh Window Function:**
> * Trong `dbt/models/silver/silver_tickets.sql`, điều kiện lọc `where {{ normalize_priority('priority_raw') }} is not null` bắt buộc phải đặt **TRƯỚC** hàm `row_number() over (partition by ticket_id order by event_time desc)`. 
> * Nhờ vậy, nếu một ticket có bản ghi mới nhất bị lỗi nhưng trước đó đã có bản ghi hợp lệ, hệ thống sẽ tự động bỏ qua bản ghi lỗi và lấy bản ghi hợp lệ gần nhất, giúp bảo toàn trọn vẹn số lượng **12,480 tickets** mà không làm mất ticket.

### 3.4. Bằng chứng đối chiếu kỹ thuật (Evidence)

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ SO SÁNH TRƯỚC VÀ SAU KHI ÁP DỤNG DATA CONTRACT & QUARANTINE ROUTING                     │
├──────────────────────────┬─────────────────────────────┬───────────────────────────────┤
│ Tiêu chí đo lường        │ Trước khi sửa (Naive Cast)  │ Sau khi sửa (Contract/Quarantine)│
├──────────────────────────┼─────────────────────────────┼───────────────────────────────┤
│ Số bản ghi cách ly       │ 0 hàng (where false)        │ 312 hàng (100% khớp kỳ vọng)  │
│ Số tickets tầng Silver   │ 6,606 hàng bị NULL / sai    │ 12,480 tickets sạch, 0 NULL   │
│ Ràng buộc Priority       │ Lẫn lộn chuỗi, 0, -1, NULL  │ 100% giá trị ∈ {1, 2, 3, 4}   │
│ Data Contract Enforced   │ False (Không cưỡng chế)     │ True (Cưỡng chế chặt chẽ)     │
│ Kết quả dbt test         │ 9 tests                     │ 11/11 tests PASS hoàn toàn    │
└──────────────────────────┴─────────────────────────────┴───────────────────────────────┘
```

<details>
<summary><b>🔍 Trích đoạn Code Diffs & Log 11/11 dbt test</b></summary>

**1. Macro chuẩn hoá tại `dbt/macros/normalize_priority.sql`:**
```sql
{% macro normalize_priority(col) %}
    case
        when {{ col }} in ('1', '2', '3', '4') then try_cast({{ col }} as integer)
        when lower({{ col }}) = 'urgent' then 1
        when lower({{ col }}) = 'high' then 2
        when lower({{ col }}) = 'medium' then 3
        when lower({{ col }}) = 'low' then 4
        else null
    end
{% endmacro %}
```

**2. Khai báo Data Contract tại `dbt/models/silver/schema.yml`:**
```yaml
  - name: silver_tickets
    config:
      contract:
        enforced: true
    columns:
      - name: ticket_id
        data_type: varchar
      - name: priority
        data_type: integer
        tests:
          - not_null
          - accepted_values:
              values: [1, 2, 3, 4]
              quote: false
```

**3. Chi tiết kết quả thực thi 11/11 dbt tests:**
```text
Found 7 models, 11 data tests, 3 sources, 502 macros
Concurrency: 4 threads (target='dev')

PASS not_null_gold_doc_chunks_chunk_id ........................... [PASS in 0.11s]
PASS not_null_silver_events_event_date ........................... [PASS in 0.12s]
PASS accepted_values_silver_tickets_priority__1__2__3__4 ......... [PASS in 0.11s]
PASS not_null_silver_events_event_id ............................. [PASS in 0.12s]
PASS not_null_silver_tickets_priority ............................ [PASS in 0.07s]
PASS not_null_silver_transcripts_transcript_id ................... [PASS in 0.05s]
PASS not_null_silver_tickets_ticket_id ........................... [PASS in 0.06s]
PASS unique_gold_doc_chunks_chunk_id ............................. [PASS in 0.07s]
PASS unique_silver_transcripts_transcript_id ..................... [PASS in 0.04s]
PASS unique_silver_tickets_ticket_id ............................. [PASS in 0.05s]
PASS unique_silver_events_event_id ............................... [PASS in 0.06s]

Done. PASS=11 WARN=0 ERROR=0 SKIP=0 TOTAL=11
```

</details>

---

## 4 · Nhiệm vụ 4: Bài Mở Rộng Kỹ Thuật (EXTRA.md)

### 4.1. Bài Mở Rộng A — Tối Ưu Hóa Truy Vấn Dashboard (Small-File Problem & Hive Partitioning)

#### Hiện trường & Điểm nghẽn hiệu năng
Mặc dù dataset `gold_events` chỉ có 130,683 dòng dữ liệu, nhưng dashboard chăm sóc khách hàng mất tới 38 giây để tải. Khi kiểm tra profiling, DuckDB phải **quét tới 5,000,000 rows**!
* **Nguyên nhân 1 (Small-file problem):** Dữ liệu bị phân mảnh thành 5.000 file Parquet nhỏ không có cấu trúc thư mục phân vùng. DuckDB chịu overhead đọc metadata của 5,000 file và kích thước đọc tối thiểu theo vector block.
* **Nguyên nhân 2 (Non-sargable query):** Điều kiện `where strftime(event_time, '%Y-%m-%d') = '2026-08-09'` bọc cột thời gian trong hàm, làm vô hiệu hóa hoàn toàn tính năng **Partition Pruning** và thống kê **Min/Max Statistics** của Parquet Row Group.

#### Giải pháp tái cấu trúc
1. **Compaction Script (`tools/compact.py`):** Gom 5.000 file nhỏ và ghi lại sang `data/gold_events_v2` sử dụng tính năng Hive Partitioning theo ngày `partition_by = (event_date)`, sắp xếp dữ liệu `order by customer_name, event_time` và đặt `row_group_size = 10000`.
2. **Sargable Query (`queries/dashboard.sql`):** Viết lại câu truy vấn trỏ tới thư mục `data/gold_events_v2/**/*.parquet` với `hive_partitioning=1` và lọc trực tiếp trên cột phân vùng `event_date = '2026-08-09'`.

#### Bằng chứng đối chiếu hiệu năng (`tools/explain.py`)

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ KẾT QUẢ PROFILING DASHBOARD QUERY (TOOLS/EXPLAIN.PY)                                   │
├──────────────────────────┬─────────────────────────────┬───────────────────────────────┤
│ Chỉ số hiệu năng         │ Trước tối ưu (5,000 files)  │ Sau tối ưu (Hive Partitioned) │
├──────────────────────────┼─────────────────────────────┼───────────────────────────────┤
│ Số lượng file Parquet    │ 5,000 file nhỏ              │ 14 file phân vùng             │
│ Số rows thực tế quét     │ 5,000,000 rows              │ 140,126 rows (Giảm 35.7×)     │
│ Ngưỡng yêu cầu           │ N/A                         │ ≤ 500,000 rows (Vượt xa ✓)    │
│ Thời gian thực thi       │ ~38,000 ms                  │ 8.3 ms (Phản hồi tức thì)     │
│ Result Hash              │ 4379e4c5d9f3                │ 4379e4c5d9f3 (Giữ nguyên 100%)│
└──────────────────────────┴─────────────────────────────┴───────────────────────────────┘
```

<details>
<summary><b>🔍 Trích xuất Terminal Output từ <code>tools/explain.py</code></b></summary>

```text
  queries/dashboard.sql
  --------------------------------------------------------------
                             TRƯỚC        HIỆN TẠI      MỤC TIÊU
  rows scanned           5,000,000         140,126     ≤ 500,000   ✓
  rows on disk             130,683         130,683   (tham khảo)
  files                      5,000              14        ít hơn   ✓
  result hash         4379e4c5d9f3    4379e4c5d9f3     không đổi   ✓
  thời gian (ms)                 —             8.3   (tham khảo)

  => giảm 35.7× (cần ≥ 10×)

  kết quả truy vấn (1 hàng):
    ('ACME', 3500, 3068, 2521.1, 4691, 262, 7764750)
```

</details>

---

### 4.2. Bài Mở Rộng B — Chịu Lỗi Tiến Trình Streaming Consumer (Delivery Semantics & Idempotent Upsert)

#### Hiện trường sự cố & Lý thuyết Delivery Semantics
Khi tiến trình Consumer đang nạp dữ liệu theo từng lô (Batch size = 500) và bị hủy đột ngột (`kill -9`) ở lô thứ 7:
* **Mã nguồn ban đầu:** Thực hiện `consumer.commit()` (lưu offset) **TRƯỚC** khi `write_batch()` ghi dữ liệu xuống kho $\to$ Ngữ nghĩa **At-most-once**. Khi bị kill ở lô 7, offset đã tăng nhưng dữ liệu chưa được ghi $\to$ **Mất vĩnh viễn 500 bản ghi**.
* **Nếu chỉ đảo thứ tự đơn thuần:** Ghi trước, commit sau nhưng giữ nguyên câu lệnh `INSERT` thuần $\to$ Ngữ nghĩa **At-least-once không an toàn**. Khi consumer khởi động lại, nó đọc lại lô 7 và chèn thêm một lần nữa $\to$ **Trùng lặp 500 bản ghi**.

#### Giải pháp Idempotent Upsert
1. Thêm ràng buộc khóa chính `event_id varchar primary key` vào định nghĩa bảng `bronze_events_stream`.
2. Thay thế `INSERT` thuần bằng cú pháp **Idempotent Upsert**:
   ```sql
   insert into bronze_events_stream values (?, ?, ?, ?, ?, ?, ?, ?)
   on conflict (event_id) do update set
       ticket_id = excluded.ticket_id,
       customer_id = excluded.customer_id,
       customer_name = excluded.customer_name,
       event_type = excluded.event_type,
       latency_ms = excluded.latency_ms,
       event_time = excluded.event_time,
       _ingested_at = excluded._ingested_at;
   ```
3. Bọc câu lệnh ghi trong **Transaction (`con.begin()` và `con.commit()`)** để tối ưu hóa I/O và đảm bảo tính nguyên tử (Atomicity).
4. Trong vòng lặp `consume()`, thiết lập thứ tự chuẩn xác: **Ghi dữ liệu trước (`write_batch`) $\to$ Commit offset sau (`consumer.commit`)**.

#### Bằng chứng đối chiếu kỹ thuật (`tools/crash_test.py`)

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ MA TRẬN ĐỐI CHIẾU CÁC CHIẾN LƯỢC DELIVERY SEMANTICS KHI BỊ KILL Ở BATCH 7              │
├──────────────────────────┬───────────────────┬────────────────────┬────────────────────┤
│ Tiêu chí đối chiếu       │ At-Most-Once      │ At-Least-Once      │ Idempotent Upsert  │
│                          │ (Code cũ)         │ (Naive Reorder)    │ (Giải pháp hoàn chỉnh)│
├──────────────────────────┼───────────────────┼────────────────────┼────────────────────┤
│ Thứ tự Commit vs Write   │ Commit trước Write│ Write trước Commit │ Write trước Commit │
│ Câu lệnh SQL             │ INSERT thuần      │ INSERT thuần       │ ON CONFLICT UPDATE │
│ Số bản ghi bị mất        │ 500 bản ghi (Lô 7)│ 0 bản ghi          │ 0 bản ghi (Không mất)│
│ Số bản ghi bị trùng      │ 0 bản ghi         │ 500 bản ghi (Lô 7) │ 0 bản ghi (Không lặp)│
│ Tổng số dòng phục hồi    │ 19,500 / 20,000   │ 20,500 / 20,000    │ 20,000 / 20,000    │
│ Kết quả kiểm thử         │ THẤT BẠI ✗        │ THẤT BẠI ✗         │ ĐẠT HOÀN HẢO ✓     │
└──────────────────────────┴───────────────────┴────────────────────┴────────────────────┘
```

<details open>
<summary><b>📄 Chi tiết Terminal Output: <code>tools/crash_test.py</code></b></summary>

```text
  topic: 20,000 message · batch 500 · giết ở lô 7

  A. chạy một mạch, không sự cố
  [consumer] đã ghi 20,000 message
     -> 20,000 hàng / 20,000 event_id khác nhau

  B. chạy và bị giết ở lô 7
  [consumer] 💥 tiến trình bị giết ở lô 7
     -> tiến trình thoát với mã 137
     -> offset đã commit: 3,000

  C. khởi động lại, chạy nốt
  [consumer] đã ghi 17,000 message
     -> 20,000 hàng / 20,000 event_id khác nhau

  ----------------------------------------------------------
  không mất bản ghi                 ✓
  không trùng bản ghi               ✓
  C == A                            ✓
  ----------------------------------------------------------
  BÀI MỞ RỘNG B: ĐẠT ✓
```

</details>

---

## 5 · Tổng Kết & 3 Nguyên Tắc Vàng Khi Tiếp Nhận Hệ Thống Mới

Khi tiếp nhận một hệ thống Data Pipeline mới trong môi trường sản xuất thực tế, tôi sẽ ưu tiên kiểm tra 3 điểm trọng yếu theo thứ tự:

1. **Kiểm tra tính Idempotency & Chiến lược Materialization:**
   * Kiểm tra mọi incremental model xem có `unique_key` và `incremental_strategy` phù hợp hay không.
   * Kiểm tra orchestrator (Airflow) để đảm bảo `catchup = False` và `max_active_runs = 1`, đảm bảo mọi thao tác re-run/backfill đều mang tính đơn định (deterministic).
2. **Kiểm tra phân bố độ trễ dữ liệu thực tế (Latency Percentiles):**
   * Không bao giờ đoán mò Lookback Window. Luôn viết truy vấn đo lường $P50, P95, P99$ giữa thời điểm phát sinh (`event_time`) và thời điểm nạp kho (`_ingested_at`) để thiết lập cửa sổ thu hồi dữ liệu chính xác, cân bằng giữa tính toàn vẹn dữ liệu và chi phí tính toán.
3. **Kiểm tra Data Contracts & Thiết lập Hàng đợi Cách ly (Quarantine / Dead Letter Queue):**
   * Đặt ranh giới hợp đồng dữ liệu nghiêm ngặt tại tầng Silver để bảo vệ các mô hình AI downstream khỏi schema drift.
   * Luôn thiết kế cơ chế định tuyến dữ liệu lỗi sang bảng Quarantine thay vì để lỗi cục bộ đánh sập toàn bộ luồng dữ liệu (High Availability).
