# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Văn Giáp |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602903` |
| GitHub | `Giappp` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-06570` (lấy phần số 02903: 2903 % 26 = 17, chỉ số bắt đầu từ 0) |

## Kết quả cuối

- `dataset_version` của bạn: `547abec84bd4` (khớp `report/evidence/run_summary.json`).
- Số khung hình train / val / test: **749 / 143 / 127**, tổng **1019** khung; số trip tương ứng **18 / 4 / 4**.
- Pipeline chạy qua cả 5 bước; `report/verify.txt` lưu kết quả **27/27 checks passed**. Kết quả này kiểm tra dữ liệu, chưa chứng minh DAG Airflow đã chạy xanh.
- **Chưa hoàn tất:** chạy DAG, bài tập gây sập/clear task, thêm câu hỏi vào dashboard và chụp ảnh. Cần bổ sung `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png` cùng ảnh log lỗi và Graph sau phục hồi trong `report/screenshots/`.

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Nguồn A dùng `ts_utc` dạng ISO UTC, còn nguồn B dùng `timestamp_ms` dạng epoch milliseconds.
Tốc độ của A là `speed_mps` (m/s), còn B là `spd_kmh` (km/h).
Các cột hướng và GPS của A là `heading_deg`, `lat`, `lon`, còn B là `hdg`, `latitude`, `longitude`.
Bàn đạp của A dùng `gas`, `brake` dạng tỷ lệ, còn B dùng `throttle_pct`, `brake_pct` dạng phần trăm; đồng hồ telemetry của B lệch +1500 ms so với video, còn A là 0 ms theo `sources.json`.
Pipeline giữ riêng `bronze.raw_telemetry_a` và `bronze.raw_telemetry_b` để bảo toàn schema từng nguồn, dễ truy vết và xử lý khác biệt ở lớp Silver.
Các cột dữ liệu CSV được giữ dạng chữ để không mất giá trị gốc, kể cả ô trống hoặc giá trị lỗi; ép kiểu ngay có thể làm ingest thất bại hoặc mất thông tin cần để kiểm tra và cách ly lỗi. Các cột metadata bổ sung như `_row_no` và `_ingested_at` vẫn có kiểu số nguyên và timestamp.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Nguồn A được đổi `ts_utc` sang timestamp UTC; nguồn B đổi tên các cột, chia tốc độ km/h cho 3.6 và bàn đạp phần trăm cho 100. Timestamp B được tính bằng `(timestamp_ms - 1500) / 1000` rồi chuyển sang UTC.
Mẫu sạch đầu tiên của B là `2025-10-08 14:30:00.009+00:00`, chỉ sau thời điểm bắt đầu video 9 ms, cho thấy độ lệch đồng hồ 1.5 giây đã được trừ.

| Drive | Trùng | Thiếu tốc độ | Tốc độ ngoài phạm vi | Tổng cách ly | Dòng sạch | Dòng nhận |
|---|---:|---:|---:|---:|---:|---:|
| drive_a | 20 | 30 | 3 | 53 | 2968 | 3021 |
| drive_b | 20 | 30 | 3 | 53 | 3181 | 3234 |

Tổng có 6149 dòng sạch và 106 dòng cách ly; sạch + cách ly bằng 6255 dòng nhận. Bảng `silver_telemetry_rejects` giữ dòng lỗi cùng lý do để kiểm toán, tìm nguyên nhân và xử lý lại khi sửa quy tắc, thay vì mất dữ liệu do xóa im lặng. Số liệu lấy từ bước 2 và `run_summary.json`.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` dùng mức `warn` vì khoảng mất tín hiệu trên 1000 ms cần được theo dõi nhưng không khiến toàn bộ telemetry sạch trở nên vô dụng; các khung không đồng bộ còn được kiểm soát ở bước sau. `in_range` dùng mức `error` vì tốc độ ngoài 0–70 m/s vi phạm điều kiện dữ liệu được phép đi tiếp.
Trong `break_gate.json`, nới `max_speed_mps` lên 1000 làm test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng**; kết quả là `PASS=5 WARN=1 ERROR=1 SKIP=0`.
Sau khi khôi phục giới hạn 70, kết quả là `PASS=6 WARN=1 ERROR=0 SKIP=0`. Cổng chất lượng kiểm tra độc lập với luật làm sạch, nên vẫn phát hiện giá trị vô lý khi luật làm sạch bị nới.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Thí nghiệm `idempotency.json` chạy lại partition `trip-06570` hai lần: toàn bảng giữ **1246 dòng** trước và sau; dấu vân tay đều là `6c7480dfde7e`.
Trip này có **45 khung**; cách ghi append hai lần vào bảng thử tạo **90 dòng**, tức nhân đôi.
Delta overwrite theo điều kiện `trip_id` thay thế đúng partition trong một commit nguyên tử. Nếu lỗi trước khi commit hoàn tất, phiên bản đã commit trước đó vẫn là trạng thái hợp lệ mà người đọc thấy; khi chạy lại thành công, chỉ partition đó được thay thế.
Cách xóa rồi ghi bằng hai thao tác riêng có thể để bảng thiếu partition nếu lỗi sau khi xóa. Thí nghiệm xác nhận dữ liệu và dấu vân tay ổn định, dù lịch sử commit Delta có thể tăng.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Pipeline coi khung có độ sáng trung bình dưới 12 là đen và loại khỏi Gold; mức từ 12 đến dưới 40 là tối, được gắn cờ nhưng không bị loại chỉ vì tối. Khung tối vẫn có thể bị loại nếu đồng thời mờ, trùng hoặc mất đồng bộ.
Kết quả có 6 khung đen ở A và 4 ở B; số khung được gắn cờ tối là 4 ở A và 61 ở B. Cần phân biệt tối với đen vì đường hầm và bóng râm là điều kiện cần có trong dữ liệu huấn luyện.
Theo `threshold.json`, tôi chọn ngưỡng 2 làm điểm khởi đầu: ngưỡng 0 bỏ sót nhiều khung gần trùng, còn ngưỡng 6 có nguy cơ loại khung có thay đổi hữu ích.
Khi xe chạy chậm, các khung kế tiếp thường giống nhau hơn; khi chạy nhanh, cảnh thay đổi mạnh hơn. Vì vậy cần kiểm tra ảnh và độ đa dạng cảnh trước khi dùng cùng ngưỡng cho mọi tốc độ, thay vì chỉ tối đa hóa số khung bị loại.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Theo `leakage.json`, **1019 khung** thuộc **26 trip** được giữ. Chia theo khung làm **26 trip** nằm trong nhiều split và **26 trip** có khung ở cả train lẫn test; chia theo trip của pipeline có **0 trip** bị chia cắt.
Các khung gần nhau trong cùng trip chia sẻ cảnh, ánh sáng và nhiều nội dung hình ảnh. Nếu xuất hiện ở cả train và test, mô hình có thể tận dụng nội dung đã thấy thay vì thể hiện khả năng tổng quát sang chuyến mới.
Điểm kiểm thử vì thế có thể cao giả tạo. Chia theo trip giảm dạng rò rỉ này, nhưng chưa bảo đảm không trùng tuyến đường hoặc cảnh giữa những trip khác nhau.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Trong mã lab, `dataset_version` là 12 ký tự đầu của SHA-256 trên danh sách `frame_id|split` được sắp theo `frame_id`; nó nhận diện tập khung và cách chia split, không phụ thuộc thời điểm chạy.
Hai lần export liên tiếp đều cho cùng phiên bản **`547abec84bd4`**, với **749 train / 143 val / 127 test**. Dùng thời gian hoặc số đếm sẽ tạo nhãn mới dù dataset không đổi, khiến việc so sánh và tái tạo khó hơn.
MSSV `2A202602903` được đưa vào `md5(trip_id || MSSV)` để xếp trip trong từng drive. Đổi MSSV có thể đổi trip thuộc mỗi split, rồi đổi phiên bản; không cần đổi các video nguồn.
Giới hạn của cách băm này là nó không trực tiếp băm toàn bộ byte ảnh hoặc mọi giá trị telemetry; muốn xác minh toàn bộ dữ liệu cần kết hợp manifest, lineage và dấu vân tay nguồn.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest tại `s3://lake/gold/datasets/547abec84bd4/manifest.json` ghi dataset có **1019 khung**, gồm **436 từ drive_a** và **583 từ drive_b**. Quy tắc loại gồm `is_black`, `is_blurry`, `is_near_dup`, `not sync_ok`; split theo trip, phân tầng theo drive và xếp bằng MD5.

| Bảng đầu vào trong lineage | Phiên bản Delta |
|---|---:|
| bronze.raw_telemetry_a | 0 |
| bronze.raw_telemetry_b | 0 |
| bronze.trips_raw | 0 |
| bronze.video_files | 1 |
| silver.frames | 27 |
| silver.telemetry | 0 |
| silver.frames_flagged | 0 |
| silver.frame_telemetry | 0 |

Bản manifest đã đọc được lưu tại `report/evidence/dataset_manifest.json`. Phiên bản `silver.frames` là 27 sau hai lần chạy lại partition trong thí nghiệm; nội dung frames và dataset_version vẫn không đổi.

SHA-256 video A: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`; video B: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`.
Để tái tạo sau 3 tháng, cần giữ file nguồn tương ứng, phiên bản dữ liệu Delta và các file lịch sử còn hiệu lực, manifest, mã nguồn/cấu hình xử lý, MSSV và phiên bản môi trường. Chỉ biết `dataset_version` mà không giữ nguồn và cấu hình thì chưa đủ.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

**Chưa thực hiện bài tập trên Airflow**, nên chưa có số task lỗi, số task `upstream_failed` hay ảnh phục hồi để báo cáo như kết quả thực nghiệm.
Trip đã xác định là **`trip-06570`**, lấy phần số `02903`: `2903 % 26 = 17`, rồi chọn phần tử thứ 17 trong danh sách trip đã sắp xếp, đánh số từ 0.
Cần đặt `LAB_FAIL_ONCE=trip-06570`, áp dụng Compose, trigger `day09_reset` rồi `day09_video_pipeline`; ghi log lỗi và trạng thái task, sau đó clear task đỏ và các task downstream bị chặn nếu cần để DAG chạy tiếp.
Cần xác nhận lại số dòng frames, kết quả verify và phiên bản sau phục hồi so với mốc **1246 dòng**, **27/27**, **`547abec84bd4`**; bổ sung ảnh log lỗi và Graph xanh. Thí nghiệm idempotency đã chứng minh chạy lại partition không nhân đôi, nhưng chưa thay thế bằng chứng phục hồi DAG. Xong phải bỏ `LAB_FAIL_ONCE` và áp dụng lại Compose.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ba nút cổ chai dự kiến khi tăng lên 500 xe/ngày là:

1. **Giải mã và xử lý video:** FFmpeg và tính chỉ số ảnh tiêu tốn CPU/I/O; DAG hiện giới hạn một task frames chạy đồng thời. Cần nhiều worker xử lý song song theo trip, hàng đợi và bộ nhớ đệm cục bộ; chỉ dùng GPU khi đo đạc cho thấy phù hợp.
2. **Biến đổi bằng DuckDB trên một máy:** CPU, RAM và việc tranh chấp file `.duckdb` giới hạn nhiều job ghi đồng thời. Cần phân hoạch dữ liệu, xử lý tăng dần và cân nhắc Spark cho ETL phân tán hoặc Trino cho truy vấn lakehouse khi khối lượng vượt máy đơn.
3. **Nạp và truy vấn warehouse:** nạp lại toàn bộ mart/gallery và truy vấn trực tiếp nhiều dòng sẽ tăng thời gian chạy và tải Postgres. Cần nạp tăng dần, phân vùng/chỉ mục và bảng tổng hợp; cân nhắc warehouse phân tích như ClickHouse hoặc BigQuery theo tải thực tế.

Đây là dự đoán từ kiến trúc hiện tại, chưa phải kết quả benchmark 500 xe. Cần đo thông lượng, thời gian chờ và dung lượng để xác định thứ tự nghẽn thực tế.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Câu hỏi: **Trip nào có tỷ lệ khung bị loại cao nhất và cần ưu tiên kiểm tra?**

```sql
SELECT drive_id, trip_id, frames, frames_kept,
       ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 2)
           AS dropped_pct
FROM marts.mart_trip_summary
ORDER BY dropped_pct DESC, trip_id
LIMIT 5;
```

SQL đã được kiểm tra trực tiếp trên Postgres warehouse; **chưa thêm vào dashboard Metabase**.

| Drive | Trip | Tổng khung | Giữ lại | Tỷ lệ loại (%) |
|---|---|---:|---:|---:|
| drive_a | trip-04622 | 60 | 26 | 56.67 |
| drive_a | trip-03904 | 22 | 11 | 50.00 |
| drive_a | trip-02086 | 60 | 32 | 46.67 |
| drive_a | trip-08888 | 60 | 36 | 40.00 |
| drive_a | trip-07774 | 34 | 21 | 38.24 |

`trip-04622` bị loại 34/60 khung, cao nhất trong kết quả. Tỷ lệ tính từ tổng khung trừ số giữ lại nên không đếm lặp khung có nhiều cờ lỗi. Truy vấn giúp ưu tiên kiểm tra trip, nhưng cần xem cờ chất lượng và ảnh để kết luận nguyên nhân.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điểm đáng chú ý là khung tối không tự động bị loại: giữ những điều kiện như đường hầm có ý nghĩa với bài toán xe tự lái. Một khó khăn thực tế đã gặp là DuckDB không có quyền ghi `/opt/work/lakehouse.duckdb`, cần sửa quyền thư mục gắn vào container.
Thí nghiệm append cho thấy một lỗi ghi đơn giản có thể nhân đôi dữ liệu dù pipeline vẫn chạy được. Những phần còn cần thực hành là phục hồi task Airflow và thêm câu hỏi vào Metabase.
Một lỗi có thể chưa bị bắt là GPS hợp lệ về kiểu dữ liệu nhưng nhảy vị trí bất hợp lý; cần thêm kiểm tra khoảng cách và tốc độ suy ra giữa các mẫu. Phần cảm nhận này cần được người làm bài rà soát và diễn đạt theo trải nghiệm của mình trước khi nộp.
