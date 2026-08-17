# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Võ Quốc Huy **Lớp:** E403 **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 27.3s
  run 2/3 … 30.7s
  run 3/3 … 32.5s

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
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
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

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

|                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**    | Khi chạy lại pipeline (hoặc Clear Task trong Airflow), số lượng bản ghi trong `gold_training_set` tăng vọt (từ 12,480 lên 38,750+ hàng), xuất hiện 12,480 ticket bị nhân bản trùng lặp dù không có lỗi runtime.                                                                                                                                                                                                                                                                                                                                   |
| **Nguyên nhân**    | Model incremental trong dbt không khai báo `unique_key` và `incremental_strategy`, khiến dbt fallback về mặc định là sinh câu lệnh `INSERT INTO` (append-only). Đồng thời nguồn CDC chứa các bản ghi cập nhật (`op='u'`) ở các ngày khác nhau, nên mỗi lần re-run hoặc chạy tiếp một ngày mới có update, dbt chèn thêm bản ghi mới thay vì cập nhật (upsert/merge) bản ghi cũ của cùng một `ticket_id`. Phía DAG Airflow để `catchup=True` và `max_active_runs=None` khiến nhiều task run đồng thời chạy bù và nhân bản dữ liệu ghi đè/chèn thêm. |
| **Cách khắc phục** | 1. Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()` của `dbt/models/gold/gold_training_set.sql`.<br>2. Cập nhật `dags/ai_training_pipeline.py` với `catchup=False` và `max_active_runs=1` để tránh backfill ngoài ý muốn và race condition khi chạy đồng thời.                                                                                                                                                                                                                                                  |
| **Bằng chứng**     | trước: 38,750 hàng (12,480 ticket bị lặp) · sau: 12,480 hàng (1 hàng / 1 ticket, 0 lặp) · checksum 3 lượt: `8dd7c98653` (100% ổn định)                                                                                                                                                                                                                                                                                                                                                                                                            |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

|                        |                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Triệu chứng**        | `gold_feature_daily` bị thiếu khoảng 5% số hàng (8,645 / 9,100 hàng), và các hàng bị thiếu tập trung ở các ngày quá khứ đã chạy xong từ lâu thay vì ngày mới nhất.                                                                                                                                                                                                                                                                                                       |
| **P99 độ trễ đo được** | **2.73 ngày** _(P50 = 0.13 ngày, P95 = 1.81 ngày, P99 = 2.73 ngày, Max = 2.94 ngày; 5.05% bản ghi trễ > 1 ngày)_                                                                                                                                                                                                                                                                                                                                                         |
| **Lookback đã chọn**   | 3 ngày — vì P99 độ trễ dữ liệu thực tế là 2.73 ngày (< 3 ngày), nên một lookback window 3 ngày (`interval 3 day`) đảm bảo quét và tổng hợp lại hơn 99% các bản ghi đến muộn (late-arriving events) mà không làm tăng tải tính toán quá mức cho các ngày quá xa.                                                                                                                                                                                                          |
| **Nguyên nhân**        | Điều kiện lọc incremental ban đầu là `where event_date > (select max(event_date) from {{ this }})`. Khi một event xảy ra ở ngày cũ (ví dụ `event_date = 08-12`) nhưng tới kho muộn (ví dụ `_ingested_at = 08-15`), tại thời điểm ngày 08-15 chạy thì `max(event_date)` trong target đã là 08-14, khiến event ngày 08-12 bị điều kiện lọc loại bỏ vĩnh viễn và không bao giờ được tổng hợp vào bảng đặc trưng.                                                            |
| **Cách khắc phục**     | 1. Sửa điều kiện lọc trong khối `is_incremental()` của `dbt/models/gold/gold_feature_daily.sql` thành lookback window 3 ngày: `where event_date >= (select max(event_date) - interval 3 day from {{ this }})`.<br>2. Khai báo composite key `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` trong `config()` để khi tính lại dữ liệu của các ngày trong window thì kết quả sẽ được ghi đè (upsert) chính xác thay vì cộng dồn số lượng. |
| **Bằng chứng**         | trước: 8,645 hàng (thiếu 455 hàng) · sau: 9,100 hàng (đủ 14 ngày × 650 customer) · checksum 3 lượt: `3db448685c` (100% ổn định)                                                                                                                                                                                                                                                                                                                                          |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Chọn P99 làm căn cứ vì P99 phản ánh độ trễ của tuyệt đại đa số dữ liệu thực tế (99%), loại bỏ các điểm dị biệt ngoại lai (outliers/anomalies). Nếu chọn `max` (hoặc cố gắng bao phủ 100%), bất kỳ một event nào bị lag hàng tháng hoặc do đồng hồ thiết bị client sai lệch nghiêm trọng cũng sẽ buộc mọi lần chạy hàng ngày phải scan và recompute lại toàn bộ lịch sử nhiều tháng/năm, gây lãng phí CPU, I/O và chi phí kho dữ liệu ở mọi lần chạy sau này. Với các bản ghi trễ vượt quá P99, chiến lược tối ưu là sử dụng định kỳ một batch backfill riêng thay vì nới rộng lookback window vĩnh viễn trong job hàng ngày.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

|                                                        |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Triệu chứng**                                        | Đội backend thay đổi kiểu cột `priority` từ số sang chuỗi (`'urgent'`, `'high'`, `'medium'`, `'low'`) từ ngày 2026-08-10. Pipeline không dừng nhưng model phân loại dự đoán kém hẳn do cột `priority` trong Silver bị biến thành NULL (6,606 hàng sai) hoặc chứa các giá trị sai dải (0, 5, -1).                                                                                                                                                                                                                                                                                                                   |
| **Nguyên nhân**                                        | Ban đầu hàm `try_cast(priority_raw as integer)` đã biến toàn bộ các nhãn chuỗi hợp lệ (`'urgent'`, `'high'`, v.v.) thành `NULL`, đồng thời lại chấp nhận các giá trị số không hợp lệ (`0`, `5`, `-1`). Data contract không được bật (`enforced: false`), và không có cơ chế phân luồng tách riêng bản ghi lỗi nên dữ liệu bẩn tràn vào Silver/Gold.                                                                                                                                                                                                                                                                |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. **Số hợp lệ (`1`, `2`, `3`, `4`)**: Đúng contract ban đầu → Giữ nguyên giá trị số.<br>2. **Nhãn chuỗi tương đương (`'urgent'`, `'high'`, `'medium'`, `'low'`)**: Schema evolution của backend, mang ngữ nghĩa đúng → Chuẩn hóa ánh xạ (`urgent` → 1, `high` → 2, `medium` → 3, `low` → 4).<br>3. **Dữ liệu không hợp lệ (`'P1'`, `'unknown'`, `0`, `5`, `-1`, `''`, `NULL`)**: Dữ liệu rác thật sự → Trả về `NULL` và đưa vào bảng `quarantine_tickets`.                                                                                                                                                        |
| **Cách khắc phục**                                     | 1. Sửa macro `dbt/macros/normalize_priority.sql` dùng khối `CASE` xử lý chuẩn hóa đủ 3 nhóm và trả về NULL cho nhóm 3.<br>2. Sửa `dbt/models/silver/silver_tickets.sql` theo nguyên tắc **lọc bản ghi lỗi trước, xếp hạng CDC sau** (`where normalize_priority is not null` trước `row_number()`) để không làm mất ticket hợp lệ.<br>3. Cập nhật `dbt/models/silver/quarantine_tickets.sql` để hứng các bản ghi có `normalize_priority is null` kèm lý do `priority_reject_reason`.<br>4. Bật `enforced: true` và kích hoạt test `accepted_values: [1, 2, 3, 4]`, `not_null` trong `dbt/models/silver/schema.yml`. |
| **Bằng chứng**                                         | `quarantine_tickets` = 312 hàng (khớp chính xác kỳ vọng) · `silver_tickets` đủ 12,480 tickets (0 ticket bị mất) · `dbt test` 11/11 pass (thêm 2 tests so với gốc)                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> 1. **Nên chặn/xử lý tại tầng Silver, không chặn ở Bronze**: Bronze đóng vai trò là "Data Lake / Raw Ingestion Zone" lưu lại nguyên vẹn mọi dữ liệu thô (raw & immutable) từ nguồn bao gồm cả timestamp `_ingested_at`. Nếu Bronze từ chối hoặc drop bản ghi lỗi ngay khi ingest, ta sẽ mất hoàn toàn dấu vết lịch sử, không thể thực hiện audit, không thể replay dữ liệu khi backend sửa lỗi, và không thể điều tra nguyên nhân sự cố. Silver là tầng làm sạch (cleaning) và áp đặt Data Contract, do đó Silver là nơi lý tưởng để chuẩn hóa và định tuyến bản ghi lỗi.<br>2. **Không để pipeline dừng khi gặp bản ghi lỗi (Dead-letter Queue / Quarantine pattern)**: Vì chỉ có 312 bản ghi CDC bị lỗi định dạng trên tổng số hơn 130,000 events và 31,200 doc chunks hợp lệ. Nếu để toàn bộ DAG dừng (fail hard), toàn bộ hệ thống phân tích, dashboard và mô hình AI của doanh nghiệp sẽ bị tê liệt chỉ vì một tỷ lệ lỗi cực nhỏ (<0.2%). Định tuyến bản ghi lỗi sang `quarantine_tickets` cho phép pipeline tiếp tục vận hành phục vụ kinh doanh, đồng thời tạo ra hàng đợi cảnh báo rõ ràng để đội vận hành xử lý sau.

---

## 4 · _(mở rộng, không bắt buộc)_ Bài trong EXTRA.md

|                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Bài đã làm**     | **Cả Bài A và Bài B** (+10 điểm thưởng)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| **Nguyên nhân**    | - **Bài A**: Dataset ban đầu gồm 5,000 file Parquet nhỏ không được partition (`small-file problem`), file layout ngẫu nhiên và câu truy vấn dashboard chứa predicate không sargable (`strftime(event_time, '%Y-%m-%d') = '2026-08-09'`), buộc DuckDB phải scan toàn bộ 5,000 files và đọc 5,000,000 rows.<br>- **Bài B**: Consumer cũ commit offset trước khi ghi dữ liệu (`at-most-once`), dẫn đến khi crash giữa batch thì mất dữ liệu. Nếu đảo thứ tự thành ghi trước commit sau (`at-least-once`) mà chỉ dùng `INSERT` thuần thì khi crash restart sẽ bị lỗi duplicate bản ghi.                                    |
| **Cách khắc phục** | - **Bài A**: Viết `tools/compact.py` thực hiện compaction sắp xếp theo `(customer_name, event_time)`, partition theo `event_date`, với `row_group_size = 2048`. Sửa `queries/dashboard.sql` trỏ vào `data/gold_events_v2/**/*.parquet` với `hive_partitioning = 1` và điều kiện sargable `event_date = '2026-08-09'`.<br>- **Bài B**: Sửa DDL thêm `event_id varchar primary key`, sửa `write_batch` dùng `INSERT INTO ... ON CONFLICT (event_id) DO UPDATE SET ...` được bọc trong transaction `con.begin()`/`con.commit()`, và xếp lại luồng trong `consume()`: `write_batch` → `maybe_crash` → `consumer.commit()`. |
| **Bằng chứng**     | - **Bài A**: Rows scanned giảm từ **5,000,000 → 9,324** (giảm **536.3×**, vượt xa yêu cầu ≥ 10×), số files giảm từ 5,000 xuống **14**, result hash giữ nguyên `4379e4c5d9f3`.<br>- **Bài B**: `make crash-test` đạt tuyệt đối: `không mất bản ghi ✓`, `không trùng bản ghi ✓`, `C == A (20,000 == 20,000) ✓`.                                                                                                                                                                                                                                                                                                          |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên                                                                                                                                                                                                                   |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1        | **Tính Idempotency của Transform & Scheduling**: Kiểm tra materialization strategy (`incremental` có khai báo `unique_key`, `merge` hay chỉ `append`), và các tham số của DAG Airflow (`catchup`, `max_active_runs`, `retries`) để đảm bảo re-run/backfill không sinh ra bản ghi trùng lặp. |
| 2        | **Phân bố độ trễ dữ liệu & Lookback Window**: Đo lường percentile độ trễ (`_ingested_at - event_time`) P50/P95/P99 để thiết lập window quét dữ liệu tới muộn (late arriving data) phù hợp, kết hợp khóa chính tổng hợp tránh cộng dồn số liệu.                                              |
| 3        | **Data Contract & Cơ chế Phân luồng Dữ liệu Lỗi (Quarantine/DLQ)**: Kiểm tra schema enforcement, data quality tests, và đảm bảo hệ thống có cơ chế phân tách bản ghi sai lệch sang vùng cách ly (quarantine) để pipeline không bị chặn đứng bởi dữ liệu lỗi cục bộ.                         |
