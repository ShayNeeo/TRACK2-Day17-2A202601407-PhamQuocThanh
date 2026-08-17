# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Phạm Quốc Thanh  **Mã sinh viên:** 2A202601407  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details open>
<summary>Output ba lần chạy nghiệm thu</summary>

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

Tổng kết: **4 / 4 tiêu chí đạt** (+ Bài mở rộng A và B đều ĐẠT).

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| Hạng mục | Nội dung chi tiết |
|---|---|
| **Triệu chứng** | Thao tác Clear Task trên Airflow hoặc chạy lại pipeline khiến số hàng bảng `gold_training_set` tăng gấp bội sau mỗi lần chạy (từ 12,480 lên 38,750 hàng sau 3 lượt), xuất hiện 12,480 ticket bị trùng lặp. |
| **Nguyên nhân** | Model khai báo `materialized='incremental'` nhưng **thiếu `unique_key` và `incremental_strategy`**. Khi chạy lại cùng một partition ngày chứa các bản ghi CDC update (`op='u'`), dbt mặc định sinh câu lệnh `INSERT` thuần thay vì `MERGE` / `DELETE+INSERT`, khiến các bản ghi cùng `ticket_id` bị ghi thêm nhiều lần. Đồng thời, cấu hình DAG có `catchup=True` và thiếu giới hạn `max_active_runs` làm tăng nguy cơ nhiều run chạy đồng thời và ghi chồng chéo dữ liệu. |
| **Cách khắc phục** | 1. [`dbt/models/gold/gold_training_set.sql`](dbt/models/gold/gold_training_set.sql): Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'delete+insert'` vào khối `config()`.<br>2. [`dags/ai_training_pipeline.py`](dags/ai_training_pipeline.py): Đặt `catchup = False` và `max_active_runs = 1`. |
| **Bằng chứng** | trước: 38,750 hàng (12,480 ticket bị lặp) · sau: 12,480 hàng (1 hàng / 1 ticket không lặp) · checksum 3 lượt: `8dd7c98653` (ổn định 100%). |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| Hạng mục | Nội dung chi tiết |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` thiếu khoảng 5% số hàng ở các ngày trong quá khứ so với đối chiếu thực tế (8,645 / 9,100 hàng). |
| **P99 độ trễ đo được** | **2.73 ngày** *(P50 = 0.13 ngày, P95 = 1.81 ngày, P99 = 2.73 ngày, Max = 2.94 ngày; tỷ lệ event đến trễ > 1 ngày là 5.05%)* |
| **Lookback đã chọn** | **3 ngày** — vì độ trễ P99 đo được là 2.73 ngày (và Max là 2.94 ngày), do đó lùi 3 ngày đảm bảo bao phủ toàn bộ các event phát sinh trong quá khứ nhưng đến kho muộn. |
| **Nguyên nhân** | Mệnh đề lọc `where event_date > (select max(event_date) from {{ this }})` chỉ lấy dữ liệu có ngày phát sinh lớn hơn ngày lớn nhất hiện có trong target. Do đó, các event xảy ra ở ngày cũ (ví dụ ngày 08-12) nhưng đến kho muộn (ngày 08-15) sẽ bị bỏ qua hoàn toàn ở các lượt chạy sau vì `08-12 < max(event_date)`. |
| **Cách khắc phục** | [`dbt/models/gold/gold_feature_daily.sql`](dbt/models/gold/gold_feature_daily.sql):<br>1. Cấu hình composite key: `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'delete+insert'`.<br>2. Mở rộng lookback window: `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`. |
| **Bằng chứng** | trước: 8,645 hàng · sau: 9,100 hàng (đủ 14 ngày × 650 khách hàng = 9,100 cặp) · checksum 3 lượt: `3db448685c`. |

### Giải thích câu hỏi thiết kế: Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> **Trả lời:**
> - **Nguyên lý:** P99 đại diện cho 99% trường hợp thực tế, giúp loại bỏ các giá trị dị biệt ngoại lai (outliers) do lỗi bất thường ở client. Nếu chọn `max` trong hệ thống thực tế (nơi có thể có bản ghi trễ cả tháng/năm do đồng hồ máy trạm sai), lookback window sẽ bị kéo dãn quá mức cần thiết.
> - **Chi phí tính toán:** Mỗi ngày nới rộng lookback window đồng nghĩa với việc ở **mọi chu kỳ chạy hàng ngày**, dbt đều phải đọc lại, tính toán lại aggregation và thực hiện `DELETE + INSERT` cho toàn bộ các ngày trong window đó. 
> - **Quyết định:** Với dataset đo được P99 = 2.73 ngày và Max = 2.94 ngày, chọn **lookback 3 ngày** là điểm cân bằng hoàn hảo: bao phủ 100% dữ liệu muộn thực tế mà chi phí tính toán tăng thêm chỉ ở mức tối thiểu.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| Hạng mục | Nội dung chi tiết |
|---|---|
| **Triệu chứng** | Team backend đổi kiểu cột `priority` sang chuỗi từ ngày 08-10 khiến `silver_tickets` có 6,606 hàng bị sai/NULL, chất lượng mô hình phân loại giảm sút; bảng `quarantine_tickets` đang rỗng (0 / 312 hàng). |
| **Nguyên nhân** | Tầng Silver thiếu Data Contract để cưỡng chế schema và thiếu logic chuẩn hoá phân biệt giữa **Schema Evolution** (đổi format biểu diễn) và **Corrupt Data** (lỗi thực sự). Biểu thức `try_cast` cũ vừa biến nhãn chuỗi hợp lệ thành NULL (mất dữ liệu tốt), vừa cho lọt các số nguyên rác như 0, 5, -1 vào Silver. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **1. Nhóm 1 (Số nguyên hợp lệ: `'1'`, `'2'`, `'3'`, `'4'`):** Giữ nguyên kiểu integer.<br>**2. Nhóm 2 (Nhãn chuỗi Schema Evolution: `'urgent'`, `'high'`, `'medium'`, `'low'`):** Map chuẩn hoá sang số nguyên tương ứng (`urgent→1, high→2, medium→3, low→4`).<br>**3. Nhóm 3 (Dữ liệu lỗi: `'0'`, `'5'`, `'-1'`, `'P1'`, `'unknown'`, `'P2'`, `''`, `NULL`):** Trả về `NULL` để đưa vào bảng cách ly `quarantine_tickets`. |
| **Cách khắc phục** | 1. [`dbt/macros/normalize_priority.sql`](dbt/macros/normalize_priority.sql): Dùng `CASE` xử lý 3 nhóm và phân loại lý do loại `priority_reject_reason`.<br>2. [`dbt/models/silver/silver_tickets.sql`](dbt/models/silver/silver_tickets.sql): Lọc `where priority_clean is not null` **trước** khi đánh số `row_number()` (loại bản ghi hỏng, bảo toàn ticket nếu có bản ghi hợp lệ trước đó).<br>3. [`dbt/models/silver/quarantine_tickets.sql`](dbt/models/silver/quarantine_tickets.sql): Lọc `where {{ normalize_priority('priority_raw') }} is null`.<br>4. [`dbt/models/silver/schema.yml`](dbt/models/silver/schema.yml): Bật `contract.enforced: true`, thêm test `not_null` và `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (khớp 100% expected) · `silver_tickets` đủ 12,480 tickets (sạch, không NULL, 1..4) · `dbt test` pass 11/11 tests. |

### Giải thích câu hỏi thiết kế: Nên chặn ở tầng Bronze hay Silver? Vì sao không để pipeline dừng khi gặp bản ghi lỗi?

> **Trả lời:**
> 1. **Chặn ở Silver thay vì Bronze:** Tầng Bronze là *Raw Landing Zone / Immutable Audit Log*, cần lưu giữ 100% dữ liệu gốc bất kể đúng hay sai để phục vụ truy vết sự cố, giải trình tuân thủ (compliance) và re-process khi có logic mới. Nếu chặn ngay ở Bronze, dữ liệu lỗi bị vứt bỏ vĩnh viễn và không thể điều tra nguyên nhân từ phía backend.
> 2. **Không để pipeline dừng (fail-fast cả DAG):** Trong hệ thống vận hành thực tế, 312 bản ghi lỗi chỉ chiếm ~2.1% lượng CDC. Nếu làm fail toàn bộ DAG, hơn 97% dữ liệu sạch của ngày và hàng chục nghìn sự kiện downstream (RAG chunks, Daily Features, Dashboard) sẽ bị trễ hạn SLA. Cơ chế **Quarantine Pattern (Dead Letter Queue)** giúp cô lập dữ liệu lỗi để xử lý riêng mà vẫn giữ cho luồng dữ liệu chính vận hành liên tục (High Availability).

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

### Bài A — Tối ưu hoá truy vấn Dashboard (Small-File Problem & Hive Partitioning)

| Hạng mục | Nội dung chi tiết |
|---|---|
| **Triệu chứng** | Dashboard CSKH chạy chậm (quét 5,000,000 rows cho dataset chỉ có ~130,683 rows). |
| **Nguyên nhân** | 5,000 file Parquet nhỏ không partition khiến engine phải mở toàn bộ 5,000 file. Đồng thời điều kiện `strftime(event_time, '%Y-%m-%d') = '2026-08-09'` không sargable (bọc cột trong hàm) nên engine không áp dụng được Partition Pruning và Row Group Statistics. |
| **Cách khắc phục** | 1. [`tools/compact.py`](tools/compact.py): Gom dataset cũ và ghi sang `data/gold_events_v2` với `partition_by = (event_date)`, sắp xếp `ORDER BY customer_name, event_time`, `row_group_size = 10000`.<br>2. [`queries/dashboard.sql`](queries/dashboard.sql): Đổi đường dẫn sang `read_parquet('data/gold_events_v2/**/*.parquet', hive_partitioning=1)` và viết lại điều kiện sargable: `customer_name = 'ACME' and event_date = '2026-08-09'`. |
| **Bằng chứng** | `rows scanned`: giảm từ 5,000,000 xuống **140,126** (**giảm 35.7×**, vượt yêu cầu ≥ 10×) · `files`: giảm từ 5,000 xuống **14 file** · `result hash`: giữ nguyên tuyệt đối `4379e4c5d9f3`. |

---

### Bài B — Xử lý sự cố Consumer giữa Batch (Delivery Semantics & Idempotent Upsert)

| Hạng mục | Nội dung chi tiết |
|---|---|
| **Triệu chứng** | Khi consumer bị kill đột ngột ở giữa batch (`kill -9`), hệ thống bị mất 500 bản ghi sau khi khởi động lại (19,500 / 20,000 hàng). |
| **Nguyên nhân** | Consumer cũ thực hiện `commit offset` **trước** khi `write_batch` (ngữ nghĩa **At-most-once**). Khi bị kill trước khi kịp ghi dữ liệu, offset đã dịch chuyển nên khi khởi động lại, consumer đọc tiếp từ vị trí mới và bỏ qua vĩnh viễn batch bị gián đoạn. Nếu chỉ đảo thứ tự mà không đổi câu lệnh `INSERT` thuần thì khi replay sẽ gây trùng lặp bản ghi (ngữ nghĩa At-least-once không idempotent). |
| **Cách khắc phục** | [`ingest/consumer.py`](ingest/consumer.py):<br>1. Thêm ràng buộc `primary key (event_id)` vào DDL `bronze_events_stream`.<br>2. Cập nhật `write_batch()` dùng `INSERT ... ON CONFLICT (event_id) DO UPDATE SET ...` bọc trong transaction.<br>3. Trong `consume()`, đảo thứ tự: **ghi dữ liệu trước (`write_batch`), commit offset sau (`consumer.commit`)**. |
| **Bằng chứng** | `make crash-test` ĐẠT 100%: Không mất bản ghi nào (`lost = 0`), không trùng bản ghi nào (`dup = 0`), kết quả phục hồi $C == A = 20,000$ hàng / 20,000 event_id duy nhất. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| **1** | Kiểm tra cấu hình **materialization & idempotency** (`unique_key`, `incremental_strategy`, tham số `catchup` / `max_active_runs` của orchestrator) để đảm bảo mọi thao tác re-run / backfill đều cho kết quả nhất quán. |
| **2** | Kiểm tra **phân bố độ trễ (P95, P99)** giữa thời điểm phát sinh sự kiện (`event_time`) và thời điểm nạp kho (`_ingested_at`) để thiết lập lookback window thích hợp, tránh mất mát dữ liệu về muộn. |
| **3** | Kiểm tra sự hiện diện của **Data Contracts & Quarantine (Dead Letter Queue)** tại các ranh giới chuyển giao (Silver layer) để bảo vệ downstream khỏi schema drift mà không làm gián đoạn toàn bộ hệ thống. |
