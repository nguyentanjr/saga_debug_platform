# Long loan saga — demo 20 bước

Hệ **tổng hợp** dựng cho buổi bảo vệ, không phải hệ của bên thứ ba. 20 service (một image Go
~10 MB, mỗi container ~4 MB RAM) nói chuyện qua Kafka thật theo kiểu choreography; observer có DB
riêng và chỉ đọc topic `loan.saga` cùng sự kiện start/die của container. Observer không biết gì
về hệ này ngoài hai file khai báo: `declarations/workflows/long-loan-saga.yaml` và
`declarations/ingestion/longsaga.yaml`.

```bash
docker compose -p longsaga -f long-saga/docker-compose.yml up -d --build
```

Dashboard: `dashboard-longsaga` trong `.claude/launch.json` (cổng 4205, proxy tới observer 29084).

| Kịch bản | Lệnh | Observer kết luận |
|---|---|---|
| Thành công 20 bước | `curl -X POST "localhost:28080/start"` | COMPLETED, 40 event, không bất thường |
| Từ chối ở bước k, hoàn tác k-1..1 | `curl -X POST "localhost:28080/start?failAt=17"` | COMPENSATED, FORWARD_STEP_FAILURE tại bước 17 |
| Bù trừ không xong | `...start?failAt=17&brokenUndo=5` | MISSING_COMPENSATION_ACK tại UNDO bước 5 |
| Service chết | `docker stop longsaga-risk-service-1` rồi `.../start` | SERVICE_CRASH (R25) tại APPROVE_RISK |
| Service sống lại | `docker start longsaga-risk-service-1` | saga chạy tiếp từ offset Kafka, COMPLETED |
| Tải | `...start?count=150&gap=40ms` | xem bảng dưới |

Đo 2026-11-03 trên build `b365af6` (observer không đổi): 200 saga (150 thành công, 50 lỗi ở 10 bước
ngẫu nhiên) = 8120 event, gửi trong 7 s → 200/200 đúng trạng thái, đúng nguyên nhân, đủ số event,
0 bất thường giả. Tắt `risk-service` giữa chừng: SERVICE_CRASH STRONG tại APPROVE_RISK.

Giới hạn: `brokenUndo` cho ra INSUFFICIENT_EVIDENCE (observer thấy bước bù trừ treo nhưng container
vẫn sống, nên không có bằng chứng về lý do — đúng ngữ nghĩa, không bịa nguyên nhân). Sau khi service
sống lại và saga hoàn tất, SAGA_STUCK vẫn được giữ như sự kiện đã xảy ra. Số đo trên hệ này là số
của hệ tổng hợp, không gộp vào số hệ thật.
