---
title: Book DeFi Data — Product Card
doc_id: CARD-book-defi-data
doc_type: product-card
verdict: showcase
version: 0.1
date: 2026-10-08
status: ร่างเพื่อพิจารณา
owner: Product owner (Book Dev)
---

# Book DeFi Data — Product Card

## 1. สรุปคุณค่าในหนึ่งประโยค

เก็บข้อมูลผลตอบแทนของ DeFi pool จาก DeFiLlama พร้อม lineage ของ chain/protocol เพื่อให้ synthesis ภายในค้นโอกาสได้

## 2. ปัญหาและผู้ใช้เป้าหมาย

- ข้อมูล yield เปลี่ยนเร็ว ต้องมี snapshot และ id ที่คงที่จึงจะเทียบย้อนหลังได้
- ผู้ใช้: เจ้าของและระบบภายใน; ถูกใช้โดย `book-opportunity-intelligence` (synthesis) และ `solo-empire-insights` (หน้าเว็บ)

## 3. สิ่งที่ทำได้จริงตอนนี้

- `ingest.py` ดึง DeFiLlama yields API → landing → Bronze; `pool_id` คงที่, dedupe ใน batch, replay จาก landing
- cron รายวัน 07:00 ผ่าน `setup_cron.sh`; `api.py` อ่านอย่างเดียว (port 8104)
- test 10 ไฟล์

## 4. หลักฐานสถานะ

- Stack: Python
- Commits: 16 (แรก 2026-08-11 → ล่าสุด 2026-10-01), branch `main`; working tree สะอาด
- Source: 22 ไฟล์ / 3,434 บรรทัด; test 10 ไฟล์
- CI: 1 workflow
- Deploy config: ไม่มี
- Live URL: ไม่พบใน repo
- โครงสร้างไฟล์เหมือนกับ repo อื่นอีก 2 ตัวในตระกูลเดียวกัน (structure signature เดียวกัน)

## 5. คุณค่าและจุดต่าง

- deterministic id และ dedupe เป็นรายละเอียดคุณภาพที่ดี
- DeFiLlama มีหน้า yields ที่ใช้งานง่ายกว่า จึงไม่มีจุดต่างต่อผู้ใช้ทั่วไป; ไม่มีมุมรายได้

## 6. ช่องว่างสำคัญ

- ไม่มี deploy config หรือ public endpoint; ใช้งานผ่าน API ในเครื่องเท่านั้น (ค่าเริ่มต้น bind 127.0.0.1)
- การให้คะแนนโอกาสเป็น heuristic ใน repo อื่น ไม่ใช่การประเมินความเสี่ยง DeFi จริง

## 7. คำตัดสิน

**showcase** — เป็นหลักฐาน data engineering ที่ดี แต่ไม่ใช่ผลิตภัณฑ์

จะเปลี่ยนเป็น `merge` ถ้ารวม repo ข้อมูลตลาดเป็นแพ็กเกจเดียว

## 8. ขั้นต่อไป

1. ตัดสินใจรวม 6 repo ข้อมูลตลาด
2. ระบุใน README ว่าไม่ใช่คำแนะนำการลงทุน

## การอนุมัติ

| บทบาท | สถานะ | วันที่ | ลงนาม |
|---|---|---|---|
| Product owner (Book Dev) | รอพิจารณา | — | ________ |

## แหล่งข้อมูล

- `README.md`
- `src/book_defi/ingest.py`
- `src/book_defi/api.py`
- `tests/`
