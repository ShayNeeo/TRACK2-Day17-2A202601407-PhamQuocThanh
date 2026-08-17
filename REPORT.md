# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Phạm Quốc Thanh  **Mã sinh viên:** 2A202601407  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details open>
<summary><b>Output ba lần chạy nghiệm thu liên tiếp</b></summary>

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

Tổng kết: **4 / 4 tiêu chí đạt** (+ Bài mở rộng A và B đều ĐẠT, tổng điểm kỳ vọng **110 / 100**).

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy (Incident #1041)

### 1. Triệu chứng
Thao tác **Clear Task** trên giao diện Airflow hoặc khi pipeline chạy lại một partition ngày khiến số lượng hàng trong bảng `gold_training_set` tăng gấp bội sau mỗi lần chạy (từ 12,480 hàng ban đầu lên 25,620 hàng ở lượt 2 và 38,750 hàng ở lượt 3). Toàn bộ 12,480 ticket bị nhân bản trùng lặp, khiến mô hình phân loại intent học trên dữ liệu sai lệch trọng số.

### 2. Root cause (Cơ chế gây ra hiện tượng)
Source dữ liệu nguồn là CDC Postgres chứa các bản ghi cập nhật `op = 'u'`. Model `gold_training_set` được khai báo `materialized = 'incremental'` nhưng **không hề khai báo `unique_key` và `incremental_strategy`**, khiến dbt mặc định biên dịch thành câu lệnh `INSERT INTO target SELECT ...` (chèn nối đuôi thuần túy). 

Khi chạy lại một partition ngày, dbt không kiểm tra trùng lặp mà ghi thêm dòng mới vào bảng đích thay vì ghi đè. **Cơ chế tại sao không thể dùng chiến lược xóa partition theo ngày:** Một ticket phát sinh vào ngày D1 nhưng được cập nhật ở ngày D2; nếu chỉ xóa partition của ngày D2 rồi chèn lại, bản ghi cũ ở ngày D1 của ticket đó vẫn tồn tại trong kho $\to$ Ticket vẫn bị nhân bản theo trục thời gian. Đồng thời, Airflow DAG đặt `catchup = True` và thiếu giới hạn `max_active_runs` khiến nhiều DAG run chạy đồng thời và ghi dồn dập vào bảng.

### 3. Cách fix (Chi tiết file sửa và cơ chế khắc phục)
1. **[`dbt/models/gold/gold_training_set.sql`](dbt/models/gold/gold_training_set.sql):**
   * *Nội dung sửa:* Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'delete+insert'` vào khối `config()`.
   * *Cơ chế khắc phục:* dbt sẽ đối chiếu `ticket_id` giữa bảng tạm (staged) và bảng đích, tự động xóa bản ghi cũ có cùng `ticket_id` trước khi chèn bản ghi mới, đảm bảo tính đơn định (Idempotent) tuyệt đối dù rerun bao nhiêu lần.
2. **[`dags/ai_training_pipeline.py`](dags/ai_training_pipeline.py):**
   * *Nội dung sửa:* Đặt `catchup = False` và `max_active_runs = 1`.
   * *Cơ chế khắc phục:* Ngăn Airflow tự động kích hoạt các run bù trong quá khứ và giới hạn chỉ duy nhất 1 DAG run được thực thi tại một thời điểm, loại bỏ hoàn toàn tranh chấp ghi đồng thời.

### 4. Bằng chứng số liệu đo đạc trước và sau khi fix
* **Trước khi fix:** Run 1 = 12,480 hàng $\to$ Run 2 = 25,620 hàng $\to$ Run 3 = 38,750 hàng (12,480 ticket bị lặp 210.5%, băm checksum bị lệch qua từng lượt).
* **Sau khi fix:** Run 1 = Run 2 = Run 3 = **12,480 hàng** (Đúng 1 hàng / 1 ticket, 0% trùng lặp).
* **Checksum hash 3 lượt:** `8dd7c98653` | `8dd7c98653` | `8dd7c98653` (Bất biến tuyệt đối ✓).
* **Kiểm tra AST Airflow DAG (`tools/check_dag.py`):** `{'catchup': False, 'max_active_runs': 1, 'ok': True}`.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ (Incident #1043)

### 1. Triệu chứng
Bảng tổng hợp đặc trưng `gold_feature_daily` bị thiếu hụt ~5% số hàng ở các ngày trong quá khứ so với thực tế (chỉ đạt 8,645 trên tổng số 9,100 cặp ngày-khách hàng). Các ngày mới chạy thì đủ dữ liệu, nhưng các ngày cũ càng lùi sâu về quá khứ càng bị thiếu hụt.

### 2. Root cause (Cơ chế gây ra hiện tượng)
Mệnh đề incremental của model sử dụng điều kiện lọc:
```sql
where event_date > (select max(event_date) from {{ this }})
```
Điều kiện này dựa trên giả định phi thực tế rằng: Sự kiện xảy ra lúc nào sẽ nạp vào kho ngay trong ngày đó (`event_time` cùng ngày với `_ingested_at`). 

Trong thực tế hệ thống phân tán, do độ trễ mạng hoặc ứng dụng client mất kết nối, có **5.05% sự kiện bị đến muộn (Late-Arriving Events)**: phát sinh ở ngày $D$ (ví dụ 08-12) nhưng tận ngày $D+3$ (08-15) mới nạp vào Bronze. Khi pipeline chạy vào ngày 08-15, giá trị `max(event_date)` trong kho đã là `2026-08-14`. Mệnh đề WHERE so sánh `08-12 > 08-14` trả về `FALSE`, khiến toàn bộ các event phát sinh trong quá khứ này **bị điều kiện lọc loại bỏ vĩnh viễn** ở các chu kỳ chạy sau.

* **P99 độ trễ đo được thực nghiệm:** **2.73 ngày** *(Chi tiết phân bố: P50 = 0.13 ngày ~3.1h, P95 = 1.81 ngày, P99 = 2.73 ngày, Max = 2.94 ngày; tỷ lệ trễ > 1 ngày là 5.05%)*.
* **Lookback đã chọn:** **3 ngày** — vì độ trễ P99 đo được là 2.73 ngày (và Max là 2.94 ngày), do đó lùi 3 ngày đảm bảo bao phủ 100% dữ liệu đến muộn mà không lãng phí chi phí tính toán.

### 3. Cách fix (Chi tiết file sửa và cơ chế khắc phục)
1. **[`dbt/models/gold/gold_feature_daily.sql`](dbt/models/gold/gold_feature_daily.sql):**
   * *Nội dung sửa 1 (Mở rộng cửa sổ):* Đổi điều kiện incremental thành:
     ```sql
     where event_date >= (select max(event_date) from {{ this }}) - interval 3 day
     ```
     *Cơ chế:* Quét lại toàn bộ các event phát sinh trong vòng 3 ngày trước ngày lớn nhất hiện có, đón trọn vẹn các event đến trễ theo phân vị P99.
   * *Nội dung sửa 2 (Composite Key & Upsert):* Cấu hình `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'delete+insert'`.
     *Cơ chế:* Vì tính toán lại 3 ngày trong quá khứ, dbt sẽ xóa các cặp `(event_date, customer_id)` cũ trong window 3 ngày và chèn lại bản ghi mới đã tổng hợp đủ dữ liệu, đảm bảo không nhân bản dòng và cập nhật lại chính xác các chỉ số thống kê (events, latency, escalations).

### 4. Bằng chứng số liệu đo đạc trước và sau khi fix
* **Trước khi fix:** 8,645 hàng (Mất 455 cặp ngày-khách hàng do event đến trễ bị loại bỏ).
* **Sau khi fix:** **9,100 hàng** (Đủ chính xác 14 ngày × 650 khách hàng = 9,100 hàng, 0 hàng bị mất).
* **Checksum hash 3 lượt:** `3db448685c` | `3db448685c` | `3db448685c` (Ổn định 100% ✓).

### Giải thích câu hỏi thiết kế: Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> **Trả lời chi tiết:**
> - **Cơ chế & Nguyên lý P99:** P99 thể hiện ngưỡng bao phủ 99% phân bố độ trễ thực tế, giúp loại bỏ các điểm ngoại lai bất thường (outliers do lỗi xung đột đồng hồ máy trạm client, buffer kẹt). 
> - **Chi phí tính toán (Compute Overhead):** Mỗi khi mở rộng Lookback Window thêm 1 ngày, ở **MỌI** chu kỳ chạy hàng ngày sau này, dbt đều phải quét lại toàn bộ dữ liệu, tính toán lại aggregation và thực hiện `DELETE + INSERT` cho toàn bộ số ngày đó. Nếu chọn `max` trong môi trường sản xuất (nơi có thể xuất hiện event trễ 30-90 ngày do client offline bật lại), chi phí tài nguyên và thời gian chạy pipeline sẽ tăng gấp hàng chục lần mà chỉ để phục vụ một vài bản ghi cá biệt.
> - **Quyết định kỹ thuật:** Với dataset đo được $P99 = 2.73\text{ ngày}$ và $Max = 2.94\text{ ngày}$ đều nằm dưới 3 ngày, lựa chọn **Lookback 3 ngày** là điểm cân bằng hoàn hảo: thu hồi 100% dữ liệu muộn thực tế mà chi phí tính toán tăng thêm chỉ ở mức tối thiểu.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ (Incident #1047)

### 1. Triệu chứng
Team backend đổi kiểu cột `priority` từ số sang chuỗi (`'urgent'`, `'high'`, `'medium'`, `'low'`) từ ngày 08-10, đồng thời để lọt các dữ liệu lỗi (`'0'`, `'5'`, `'-1'`, `'unknown'`, `'P1'`, `''`, `NULL`). Biểu thức `try_cast` cũ khiến `silver_tickets` có 6,606 hàng bị sai/NULL, làm giảm chất lượng huấn luyện mô hình AI; bảng `quarantine_tickets` rỗng hoàn toàn (0 / 312 hàng).

### 2. Root cause (Cơ chế gây ra hiện tượng)
1. **Lỗi cơ chế kép của `try_cast`:** Biểu thức `try_cast(priority_raw as integer)` vừa biến toàn bộ các nhãn chuỗi hợp lệ (`'urgent'`, `'high'`) thành `NULL` (đánh mất dữ liệu sạch do Schema Evolution), vừa cho lọt các số nguyên rác như `0`, `5`, `-1` vào Silver vì chúng ép kiểu integer thành công.
2. **Thiếu Data Contract & Hàng đợi cách ly:** Tầng Silver không có Data Contract cưỡng chế schema, và bảng `quarantine_tickets` chứa điều kiện cứng `where false` nên không thể cách ly bản ghi lỗi.
3. **Cơ chế Window Ranking:** Nếu không lọc bản ghi lỗi **trước** hàm `row_number() over (partition by ticket_id order by event_time desc)`, khi một ticket có bản ghi mới nhất bị lỗi, hàm ranking sẽ gán `rn = 1` cho bản ghi lỗi đó; khi lọc ở bước sau, ticket đó sẽ bị biến mất hoàn toàn thay vì giữ lại bản ghi lịch sử hợp lệ trước đó.

### 3. Cách fix (Chi tiết file sửa và cơ chế khắc phục)
1. **[`dbt/macros/normalize_priority.sql`](dbt/macros/normalize_priority.sql):**
   * *Nội dung sửa:* Dùng `CASE` phân loại và chuẩn hóa 3 nhóm:
     - **Nhóm 1 (Số nguyên hợp lệ `'1'..'4'`):** Giữ nguyên kiểu integer.
     - **Nhóm 2 (Schema Evolution `'urgent', 'high', 'medium', 'low'`):** Map tương ứng sang số nguyên `1, 2, 3, 4`.
     - **Nhóm 3 (Dữ liệu lỗi `'0', '5', '-1', 'unknown', 'P1', '', NULL`):** Trả về `NULL`.
2. **[`dbt/models/silver/silver_tickets.sql`](dbt/models/silver/silver_tickets.sql):**
   * *Nội dung sửa:* Đặt `where {{ normalize_priority('priority_raw') }} is not null` **trước** CTE đánh số `row_number()`.
   * *Cơ chế:* Loại bỏ bản ghi lỗi khỏi cửa sổ ranking, giúp ticket nếu có bản ghi lịch sử hợp lệ vẫn được lấy với `rn = 1`, bảo toàn đủ 12,480 tickets.
3. **[`dbt/models/silver/quarantine_tickets.sql`](dbt/models/silver/quarantine_tickets.sql):**
   * *Nội dung sửa:* Lọc `where {{ normalize_priority('priority_raw') }} is null`.
   * *Cơ chế:* Định tuyến chính xác toàn bộ 312 bản ghi lỗi vào bảng cách ly Dead Letter Queue để điều tra nguyên nhân.
4. **[`dbt/models/silver/schema.yml`](dbt/models/silver/schema.yml):**
   * *Nội dung sửa:* Bật `contract: {enforced: true}` và thêm test `not_null`, `accepted_values: [1, 2, 3, 4]`.
   * *Cơ chế:* Cưỡng chế Data Contract ở cấp độ schema, chặn đứng mọi dữ liệu vi phạm contract tràn vào tầng Gold.

### 4. Bằng chứng số liệu đo đạc trước và sau khi fix
* **Bảng `quarantine_tickets`:** Trước = 0 hàng $\to$ Sau = **312 hàng lỗi** (Khớp 100% kỳ vọng chuẩn).
* **Bảng `silver_tickets`:** Trước = 6,606 hàng sai/NULL $\to$ Sau = **12,480 tickets sạch** (0 NULL, 100% priority ∈ {1, 2, 3, 4}).
* **Kiểm thử dbt test:** PASS **11/11 tests** (0 warning, 0 error).

### Giải thích câu hỏi thiết kế: Nên chặn ở tầng Bronze hay Silver? Vì sao không để pipeline dừng khi gặp bản ghi lỗi?

> **Trả lời chi tiết:**
> 1. **Chặn ở Silver thay vì Bronze:** Tầng Bronze là *Raw Landing Zone / Immutable Audit Log*, có nhiệm vụ ghi nhận trung thực 100% dữ liệu gốc bất kể đúng hay sai để phục vụ truy vết sự cố, giải trình tuân thủ (compliance) và re-process khi backend sửa lỗi. Nếu chặn ngay ở Bronze, dữ liệu lỗi bị vứt bỏ vĩnh viễn và không thể điều tra nguyên nhân.
> 2. **Không để pipeline dừng (fail-fast cả DAG):** Trong 14,300 bản ghi CDC, chỉ có 312 bản ghi bị lỗi (chiếm ~2.1%). Nếu làm fail toàn bộ DAG, hơn 97% dữ liệu sạch của ngày và toàn bộ các tác vụ downstream (RAG vector index, Classifier training, Dashboard) sẽ bị tê liệt, vi phạm nghiêm trọng SLA. Cơ chế **Quarantine Pattern (Dead Letter Queue)** giúp cô lập dữ liệu lỗi để xử lý riêng mà vẫn duy trì tính sẵn sàng cao nhất (**High Availability**) cho luồng dữ liệu chính.

---

## 4 · Bài trong EXTRA.md (Mở rộng kỹ thuật)

### Bài A — Tối ưu hoá truy vấn Dashboard (Small-File Problem & Hive Partitioning)

#### 1. Triệu chứng
Dashboard chăm sóc khách hàng tải rất chậm (mất ~38 giây) và DuckDB phải quét tới **5,000,000 rows** cho một tập dữ liệu thực tế chỉ có 130,683 dòng.

#### 2. Root cause (Cơ chế gây ra hiện tượng)
1. **Small-file problem:** Dữ liệu bị phân mảnh thành 5.000 file Parquet nhỏ không có cấu trúc thư mục phân vùng $\to$ Engine phải đọc metadata của 5,000 file độc lập, chịu overhead kích thước vector block đọc tối thiểu (1,000 rows/file).
2. **Non-sargable query:** Mệnh đề `where strftime(event_time, '%Y-%m-%d') = '2026-08-09'` bọc cột thời gian trong hàm $\to$ Khiến engine không thể áp dụng kỹ thuật **Partition Pruning** và thống kê **Min/Max Statistics** của Parquet Row Group, buộc phải quét toàn bộ mọi file.

#### 3. Cách fix (Chi tiết file sửa và cơ chế khắc phục)
1. **[`tools/compact.py`](tools/compact.py):**
   * *Nội dung sửa:* Dùng `COPY ... TO 'data/gold_events_v2' (FORMAT parquet, PARTITION_BY (event_date), ROW_GROUP_SIZE 10000)` với `order by customer_name, event_time`.
   * *Cơ chế:* Gom 5,000 file nhỏ thành 14 file Hive Partition theo ngày, sắp xếp dữ liệu để tối ưu hóa khoảng Min/Max metadata cho từng Row Group.
2. **[`queries/dashboard.sql`](queries/dashboard.sql):**
   * *Nội dung sửa:* Đổi nguồn đọc sang `read_parquet('data/gold_events_v2/**/*.parquet', hive_partitioning=1)` và viết lại điều kiện sargable: `customer_name = 'ACME' and event_date = '2026-08-09'`.
   * *Cơ chế:* DuckDB tự động bỏ qua 13 partition ngày không khớp (Partition Pruning) và dùng Min/Max stats của cột `customer_name` để bỏ qua các row group không phải 'ACME', giảm mạnh I/O disk.

#### 4. Bằng chứng số liệu đo đạc (`tools/explain.py`)
* **Số rows quét (Rows Scanned):** 5,000,000 $\to$ **140,126 rows** (**Giảm 35.7×**, mục tiêu $\ge 10\times$ ✓).
* **Số lượng file Parquet:** 5,000 file $\to$ **14 file phân vùng**.
* **Thời gian thực thi:** ~38,000 ms $\to$ **8.3 ms**.
* **Kết quả truy vấn (Result Hash):** `4379e4c5d9f3` (Khớp tuyệt đối 100% không đổi ✓).

---

### Bài B — Xử lý sự cố Consumer giữa Batch (Delivery Semantics & Idempotent Upsert)

#### 1. Triệu chứng
Khi consumer đang nạp dữ liệu theo lô (Batch size = 500) và bị hủy tiến trình đột ngột (`kill -9`) ở lô thứ 7, hệ thống bị mất 500 bản ghi sau khi khởi động lại (chỉ thu được 19,500 / 20,000 hàng).

#### 2. Root cause (Cơ chế gây ra hiện tượng)
1. **Mã nguồn ban đầu (At-most-once):** Thực hiện `consumer.commit()` (lưu offset) **TRƯỚC** khi `write_batch()` ghi dữ liệu xuống kho. Khi bị kill ở lô 7, offset đã lưu vị trí mới (3,500) nhưng dữ liệu chưa được ghi $\to$ Khi restart, consumer đọc tiếp từ 3,500 và bỏ rơi vĩnh viễn 500 bản ghi của lô 7.
2. **Nếu chỉ đảo thứ tự (At-least-once không an toàn):** Ghi trước, commit sau nhưng giữ nguyên câu lệnh `INSERT` thuần $\to$ Khi bị kill trước khi commit, restart sẽ đọc lại lô 7 và chèn thêm 500 dòng mới $\to$ Gây trùng lặp dữ liệu (20,500 hàng).

#### 3. Cách fix (Chi tiết file sửa và cơ chế khắc phục)
1. **[`ingest/consumer.py`](ingest/consumer.py):**
   * *Nội dung sửa 1:* Thêm `primary key (event_id)` vào DDL `bronze_events_stream`.
   * *Nội dung sửa 2:* Dùng cú pháp **Idempotent Upsert** bọc trong transaction:
     ```sql
     insert into bronze_events_stream values (?, ?, ?, ?, ?, ?, ?, ?)
     on conflict (event_id) do update set
         ticket_id     = excluded.ticket_id,
         customer_id   = excluded.customer_id,
         customer_name = excluded.customer_name,
         event_type    = excluded.event_type,
         latency_ms    = excluded.latency_ms,
         event_time    = excluded.event_time,
         _ingested_at  = excluded._ingested_at;
     ```
     *Cơ chế:* Đảm bảo tính Idempotent ở tầng lưu trữ; nếu một batch được replay lại, database sẽ ghi đè/update thay vì chèn dòng mới.
   * *Nội dung sửa 3:* Trong `consume()`, thiết lập thứ tự chuẩn: **Ghi dữ liệu trước (`write_batch`) $\to$ Commit offset sau (`consumer.commit`)**.
     *Cơ chế:* Đảm bảo không bao giờ mất message (At-least-once) và kết hợp với Idempotent Upsert để đạt kết quả tương đương Exactly-once.

#### 4. Bằng chứng số liệu đo đạc (`tools/crash_test.py`)
* **Số bản ghi bị mất (Lost):** `0 bản ghi`.
* **Số bản ghi bị trùng (Dup):** `0 bản ghi`.
* **Kết quả phục hồi sau crash:** **20,000 / 20,000 message** ($C == A$, BÀI MỞ RỘNG B: ĐẠT ✓).

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| **1** | Kiểm tra cấu hình **materialization & idempotency** (`unique_key`, `incremental_strategy`, tham số `catchup` / `max_active_runs` của orchestrator) để đảm bảo mọi thao tác re-run / backfill đều cho kết quả nhất quán. |
| **2** | Kiểm tra **phân bố độ trễ (P95, P99)** giữa thời điểm phát sinh sự kiện (`event_time`) và thời điểm nạp kho (`_ingested_at`) để thiết lập lookback window thích hợp, tránh mất mát dữ liệu về muộn. |
| **3** | Kiểm tra sự hiện diện của **Data Contracts & Quarantine (Dead Letter Queue)** tại các ranh giới chuyển giao (Silver layer) để bảo vệ downstream khỏi schema drift mà không làm gián đoạn toàn bộ hệ thống. |
