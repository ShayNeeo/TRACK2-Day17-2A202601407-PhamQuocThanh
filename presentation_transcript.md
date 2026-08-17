# KỊCH BẢN THUYẾT TRÌNH: CHUYỆN KỂ TỪ CA TRỰC PIPELINE
## "Một Đêm Trực Sóng Gió & Những Bài Học Xương Máu Trong Data Engineering"

**Người trình bày:** Phạm Quốc Thanh (2A202601407)  
**Thời lượng dự kiến:** 12 - 15 phút  
**Phong cách:** Storytelling (Kể chuyện thực chiến, chia sẻ góc nhìn kỹ thuật & trade-offs, tương tác trực tiếp với số liệu mô phỏng).

---

### [00:00 - 01:30] MỞ ĐẦU: HIỆN TRƯỜNG VỤ ÁN LÚC 2 GIỜ SÁNG

*(Mở Slide 1: Bối cảnh & Hiện trường)*

> **Lời thoại:**
> 
> *"Xin chào thầy cô và các bạn. Hôm nay tôi không đứng đây để đọc lại lý thuyết dbt hay định nghĩa lại data pipeline là gì. Tôi muốn mời các bạn cùng tôi quay trở lại một ca trực đêm thứ Sáu — ca trực mà bất kỳ ai làm Data Engineer hay MLOps sớm muộn gì cũng sẽ phải đối mặt.*
> 
> *Hãy tưởng tượng bạn vừa tiếp nhận bàn giao một hệ thống dữ liệu phục vụ nền tảng AI Chăm sóc khách hàng. Dữ liệu chảy từ CDC Postgres, S3 transcripts và Kafka events, qua Bronze, Silver rồi đổ vào 3 bảng Gold:*
> 1. *`gold_doc_chunks` nạp vào vector DB cho RAG.*
> 2. *`gold_training_set` huấn luyện mô hình phân loại intent.*
> 3. *`gold_feature_daily` cung cấp đặc trưng thời gian thực cho Routing Agent.*
> 
> *Khi bạn chạy thử nghiệm: `dbt test` trả về màu xanh mướt — 9/9 test pass, không một dòng traceback báo lỗi. Mọi thứ trông có vẻ hoàn hảo! Nhưng đến sáng hôm sau:*
> - *Đội MLOps báo: Model Classifier bỗng nhiên dự đoán sai lệch hoàn toàn.*
> - *Đội CSKH gọi điện: Dashboard giám sát mất tận 38 giây mới load xong.*
> - *Và bảng số liệu tổng hợp doanh thu theo ngày bị hụt mất 5% dữ liệu.*
> 
> *Một nghịch lý cay đắng trong nghề Data: **Một pipeline chạy không báo lỗi không có nghĩa là nó đang chạy đúng.** Hãy cùng tôi đi qua 3 phiếu sự cố và 2 bài toán tối ưu để xem chúng ta đã tìm ra thủ phạm như thế nào."*

---

### [01:30 - 04:00] PHẦN 1: CON QUÁI VẬT NHÂN BẢN & BÀI HỌC IDEMPOTENCY

*(Chuyển Slide 2: Sự cố #1041 - `gold_training_set`)*

> **Lời thoại:**
> 
> *"Sự cố đầu tiên bắt đầu từ một nút bấm tưởng chừng vô hại trên giao diện Airflow: **Clear Task**.*
> 
> *Đêm đó mạng chập chờn, một task bị timeout. Người trực ca trước bấm Clear Task để chạy lại ngày hôm đó. Sáng hôm sau mở kho ra, bảng `gold_training_set` từ 12,480 hàng bỗng nhiên nhảy vọt lên 25,000 hàng, rồi chạy thêm lần nữa nó lên 38,750 hàng! 12,480 ticket bị nhân bản hàng loạt.*
> 
> *Tại sao lại như vậy?*
> 
> *Khi mở file `gold_training_set.sql`, tôi thấy cấu hình `materialized = 'incremental'`, nhưng lại **không hề khai báo `unique_key`**. Trong dbt, khi bạn bảo nó chạy incremental mà không đưa khoá chính, nó mặc định làm gì? Nó chỉ sinh ra một câu `INSERT INTO` nối đuôi! Cứ mỗi lần chạy lại hay backfill, nó lại nhét thêm một đống hàng trùng vào.*
> 
> *Nhưng câu hỏi hóc búa ở đây là: **Tại sao không dùng chiến lược xoá partition ngày rồi insert lại (delete+insert theo ngày)?**
> 
> *Câu trả lời nằm ở bản chất dữ liệu nguồn: Đây là **CDC (Change Data Capture)** từ Postgres, có các bản ghi update `op = 'u'`. Một ticket được tạo ở ngày D1, nhưng bị sửa đổi ở ngày D2 và D5. Nếu bạn chỉ xoá partition của ngày D2, bạn vẫn giữ bản ghi ngày D1 và D5 của cùng ticket đó $\to$ Bảng vẫn bị nhân bản theo thời gian!*
> 
> *Giải pháp duy nhất đúng: Khai báo `unique_key = 'ticket_id'` và `incremental_strategy = 'delete+insert'`. Đồng thời trên Airflow, phải khoá `catchup = False` và `max_active_runs = 1`.*
> 
> *(Chỉ vào nút mô phỏng trên slide)*: *Các bạn có thể bấm thử nút mô phỏng trên màn hình: Khi chưa có Idempotency, mỗi lần bấm Rerun số hàng tăng đột biến; nhưng khi bật `unique_key`, dù bạn có bấm 100 lần, bảng vẫn giữ nguyên đúng 12,480 hàng với checksum tuyệt đối không đổi.*
> 
> *Bài học rút ra: **Tính Idempotent (chạy lại N lần vẫn cho một kết quả) là tính chất sống còn số 1 của data pipeline.**"*

---

### [04:00 - 06:45] PHẦN 2: BÓNG MA DỮ LIỆU VỀ MUỘN & CON SỐ P99 ĐẮT GIÁ

*(Chuyển Slide 3: Sự cố #1043 - `gold_feature_daily`)*

> **Lời thoại:**
> 
> *"Sang sự cố thứ hai: Đội vận hành phát hiện bảng `gold_feature_daily` bị thiếu mất 5% dữ liệu (chỉ có 8,645 trên tổng số 9,100 cặp ngày-khách hàng). Nhưng điều kỳ quái là: **Những ngày mới chạy thì đủ, chỉ có những ngày trong quá khứ là bị thiếu!**
> 
> *Dữ liệu đã biến đi đâu?*
> 
> *Hãy nhìn vào điều kiện incremental cũ: `where event_date > (select max(event_date) from target)`. Điều kiện này ngây thơ giả định rằng: Sự kiện xảy ra lúc nào thì mạng và máy chủ sẽ gửi nó đến kho dữ liệu ngay lúc đó!*
> 
> *Nhưng thực tế đời không như mơ: Thiết bị di động của khách hàng bị mất sóng, ứng dụng bị crash, sự kiện xảy ra ngày 08-12 nhưng tận ngày 08-15 mới sync về tới kho. Đến ngày 08-15, `max(event_date)` trong kho đã là ngày 08-14 rồi. Câu lệnh WHERE thấy `08-12 < 08-14` liền gạt phăng bản ghi đó ra ngoài! Dữ liệu bị bỏ rơi vĩnh viễn.*
> 
> *Vậy giải quyết thế nào? Chúng ta cần một **Lookback Window** (khoảng thời gian nhìn lại). Nhưng lùi lại bao nhiêu ngày?*
> 
> *Nếu bạn đoán mò: 'Thôi lùi đại 30 ngày cho chắc!' $\to$ Chi phí là gì? Cứ mỗi 2 giờ sáng, pipeline lại phải lôi 30 ngày dữ liệu cũ ra tính toán lại toàn bộ, tốn tiền compute và nghẽn hệ thống.*
> 
> *Thay vì đoán, chúng ta dùng phương pháp định lượng: Truy vấn trực tiếp phân bố độ trễ giữa `event_time` và `_ingested_at`:*
> - *$P50 = 0.13\text{ ngày}$ (khoảng 3 tiếng).*
> - *$P95 = 1.81\text{ ngày}$.*
> - **$P99 = 2.73\text{ ngày}$**.
> - *$Max = 2.94\text{ ngày}$.*
> - *Tỷ lệ trễ > 1 ngày đúng bằng **5.05%** — giải thích chính xác con số 5% bị thiếu hụt!*
> 
> *Con số $P99 = 2.73\text{ ngày}$ chính là bằng chứng vàng để chúng ta tự tin chọn **Lookback 3 ngày**. Kết hợp với composite key `['event_date', 'customer_id']`, chúng ta vừa thu hồi trọn vẹn 100% dữ liệu muộn (đạt đủ 9,100 hàng), vừa kiểm soát chi phí tính toán ở mức tối ưu nhất."*

---

### [06:45 - 09:30] PHẦN 3: SỰ ĐỔI THAY CỦA SCHEMA & BỨC TƯỜNG CÁCH LY (DEAD LETTER QUEUE)

*(Chuyển Slide 4: Sự cố #1047 - `silver_tickets` & `quarantine`)*

> **Lời thoại:**
> 
> *"Sự cố thứ ba là câu chuyện kinh điển giữa Backend và Data team: Đội Backend âm thầm đổi kiểu cột `priority` từ số (1..4) sang chữ ('urgent', 'high', 'medium', 'low') và kèm theo một số bản ghi rác như 'P1', 'unknown', '0', '-1'.*
> 
> *Hệ thống cũ xử lý thế nào? Nó dùng `try_cast(priority_raw as integer)`. Kết quả là gì?*
> 1. *Toàn bộ nhãn chữ 'urgent', 'high' bị ép thành NULL $\to$ Đánh mất hơn 7,000 bản ghi hoàn toàn tốt.*
> 2. *Các số rác như '0', '5', '-1' lại được cho qua vì chúng... đúng là số nguyên!*
> 
> *Khi đối mặt với sự cố này, một kỹ sư thiếu kinh nghiệm sẽ chọn giải pháp nào? 'Thêm constraint, nếu gặp dữ liệu sai thì cho pipeline dừng ngay (Fail-Fast)!'*
> 
> *Nhưng hãy đặt mình vào bài toán kinh doanh: Có 312 bản ghi lỗi trên tổng số 14,300 bản ghi. Nếu bạn làm sập pipeline, hơn 97% khách hàng còn lại và hàng chục nghìn tài liệu RAG của công ty sẽ bị ngưng trệ! Không ai muốn đánh sập cả nhà máy chỉ vì một vài chi tiết lỗi trên băng chuyền.*
> 
> *Giải pháp kiến trúc chuẩn:*
> 1. **Phân loại rạch ròi:**
>    - *Nhóm 1: Số hợp lệ (1..4) $\to$ Giữ nguyên.*
>    - *Nhóm 2: Schema Evolution ('urgent' $\to$ 1, 'high' $\to$ 2...) $\to$ Map về chuẩn.*
>    - *Nhóm 3: Dữ liệu lỗi thật $\to$ Trả về NULL.*
> 2. **Quarantine Pattern (Hàng đợi cách ly):** Định tuyến 312 bản ghi lỗi này vào bảng riêng `quarantine_tickets` để kỹ sư trực tra cứu nguyên nhân, trong khi luồng chính vẫn tiếp tục chạy.
> 3. **Lọc trước khi deduplicate:** Trong `silver_tickets`, phải lọc bản ghi lỗi **trước** hàm `row_number()`. Nhờ đó, nếu một ticket có bản ghi mới nhất bị lỗi, hệ thống vẫn giữ lại trạng thái hợp lệ trước đó của ticket (bảo toàn đủ 12,480 ticket thay vì bị mất oan).*
> 4. **Data Contract:** Bật `contract.enforced: true` và `accepted_values: [1, 2, 3, 4]` để bảo vệ tuyệt đối tầng Silver."*

---

### [09:30 - 12:00] PHẦN 4: BÀI TOÁN TỐI ƯU & SINH TỬ KHI SỰ CỐ XẢY RA (EXTRA A & B)

*(Chuyển Slide 5 & 6: Bài mở rộng)*

> **Lời thoại:**
> 
> *"Cuối cùng, hãy nhìn vào hai bài toán nâng cao mà chúng tôi đã giải quyết:*
> 
> #### Bài A: Vấn đề 5.000 file tí hon (Small-file problem)
> *Dashboard CSKH load 38s là vì DuckDB phải mở 5,000 file Parquet nhỏ không partition. Chúng tôi viết `tools/compact.py` để gom lại thành 14 file theo Hive Partition `event_date`, sắp xếp thứ tự `customer_name, event_time` và viết lại mệnh đề WHERE sargable.
> Kết quả: **Số rows scanned giảm từ 5 triệu xuống 140 nghìn (giảm 35.7 lần), thời gian truy vấn chỉ còn 18ms!**
> 
> #### Bài B: Consumer bị 'Kill -9' giữa chừng (Delivery Semantics)
> *Nếu consumer commit offset trước khi ghi dữ liệu (At-most-once) $\to$ Bị kill sẽ mất 500 hàng.*
> *Nếu ghi trước commit sau mà dùng INSERT thường (At-least-once) $\to$ Bị kill sẽ trùng 500 hàng.*
> *Chúng tôi đã thiết lập **At-least-once kết hợp Idempotent Upsert (`ON CONFLICT DO UPDATE`) trong transaction** $\to$ Crash test đạt tuyệt đối: 0 mất, 0 trùng, phục hồi 20,000 / 20,000 message nguyên vẹn."*

---

### [12:00 - 13:30] TỔNG KẾT: 3 NGUYÊN TẮC VÀNG CỦA DATA ENGINEER

*(Chuyển Slide 7: Tổng kết)*

> **Lời thoại:**
> 
> *"Thưa thầy cô và các bạn, 4/4 tiêu chí đạt và 110/100 điểm hôm nay không phải là đích đến, mà quan trọng nhất là 3 nguyên tắc bất di bất dịch khi xây dựng bất kỳ hệ thống dữ liệu nào:*
> 
> 1. **Idempotency là tôn chỉ:** Mọi pipeline phải được thiết kế để có thể chạy lại bất kỳ lúc nào mà không sợ nhân bản dữ liệu.
> 2. **Định lượng thay vì cảm tính:** Đừng đoán lookback window — hãy đo P99 của độ trễ dữ liệu để tối ưu giữa tính đầy đủ và chi phí tài nguyên.
> 3. **Data Contract & Quarantine:** Bảo vệ dữ liệu hạ tầng bằng hợp đồng nghiêm ngặt, nhưng hãy dùng hàng đợi cách ly để hệ thống luôn duy trì tính sẵn sàng cao nhất (High Availability).
> 
> *Xin chân thành cảm ơn thầy cô và các bạn đã lắng nghe!"*

---
