---
title: Book Stock Data — Product Card
doc_id: CARD-book-stock-data
doc_type: product-card
verdict: showcase
version: 0.1
date: 2026-10-08
status: ร่างเพื่อพิจารณา
owner: Product owner (Book Dev)
---

# Book Stock Data — Product Card

## 1. สรุปคุณค่าในหนึ่งประโยค

ให้ข้อมูลราคาหุ้นที่มี lineage สำหรับ dashboard และ synthesis ภายใน Solo Empire ตามแบบเดียวกับ `book-crypto-data`

## 2. ปัญหาและผู้ใช้เป้าหมาย

- ต้องการข้อมูลหุ้นที่ replay และตรวจย้อนได้ในระบบส่วนตัว
- ผู้ใช้: เจ้าของและระบบภายใน; ถูกใช้โดย `book-opportunity-intelligence` (synthesis) และ `solo-empire-insights` (หน้าเว็บ)

## 3. สิ่งที่ทำได้จริงตอนนี้

- `ingest.py` ดึง Yahoo public chart endpoint (best-effort) → landing → Bronze → manifest → CSV ทางเลือก
- `api.py` อ่าน Bronze ผ่าน DuckDB (port 8102) และมีโหมด Silver parity แบบมีเงื่อนไข
- test 10 ไฟล์

## 4. หลักฐานสถานะ

- Stack: Python
- Commits: 14 (แรก 2026-08-08 → ล่าสุด 2026-10-01), branch `main`; working tree สะอาด
- Source: 20 ไฟล์ / 3,114 บรรทัด; test 10 ไฟล์
- CI: 1 workflow
- Deploy config: ไม่มี
- Live URL: ไม่พบใน repo
- โครงสร้างไฟล์เหมือนกับ repo อื่นอีก 2 ตัวในตระกูลเดียวกัน (structure signature เดียวกัน)

## 5. คุณค่าและจุดต่าง

- พิสูจน์ว่า shared adapter ใช้ซ้ำได้ข้ามโดเมน
- Yahoo endpoint ไม่ใช่ API ทางการ จึงไม่มั่นคงพอสำหรับผลิตภัณฑ์ภายนอก; ไม่มีมุมรายได้โดยตรง

## 6. ช่องว่างสำคัญ

- ไม่มี deploy config หรือ public endpoint; ใช้งานผ่าน API ในเครื่องเท่านั้น (ค่าเริ่มต้น bind 127.0.0.1)
- แหล่งข้อมูล best-effort อาจหยุดทำงานเมื่อใดก็ได้
- โครงซ้ำกับ fx/flight (structure signature เดียวกัน)

## 7. คำตัดสิน

**showcase** — ทำงานจริงและเป็นหลักฐานการใช้ pattern ซ้ำ แต่ไม่ใช่ธุรกิจและพึ่งแหล่งที่ไม่เป็นทางการ

จะเปลี่ยนเป็น `merge` ถ้าตัดสินใจรวม repo ข้อมูลตลาดเป็นแพ็กเกจเดียว

## 8. ขั้นต่อไป

1. ระบุข้อจำกัดของแหล่งข้อมูลใน README ให้ชัด
2. ตัดสินใจรวม 6 repo ข้อมูลตลาด

## การอนุมัติ

| บทบาท | สถานะ | วันที่ | ลงนาม |
|---|---|---|---|
| Product owner (Book Dev) | รอพิจารณา | — | ________ |

## แหล่งข้อมูล

- `README.md`
- `src/book_stock/api.py`
- `src/book_stock/ingest.py`
- `tests/`
