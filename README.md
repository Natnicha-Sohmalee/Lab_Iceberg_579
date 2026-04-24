# Healthcare Data Pipeline (Parquet vs Iceberg)

## Structure

```id="a1"
project/
├── data/healthcare_dataset.csv
├── scripts/
│   ├── parquet_pipeline.py
│   └── iceberg_pipeline.py
├── output/        # parquet result
└── warehouse/     # iceberg table
```

---

## Run

```id="a2"
python scripts/parquet_pipeline.py
python scripts/iceberg_pipeline.py
```

---

## 🔹 Parquet

* Read CSV → filter (`Kyle Wiley`) → overwrite
* No row-level delete

---

## 🔹 Iceberg

* Create table → `DELETE FROM`
* Row-level delete
* Snapshot (versioning)

---

## Result

* Parquet: rewrite file
<img width="1619" height="718" alt="image" src="https://github.com/user-attachments/assets/be72dcab-a71e-4533-b930-22fd8571c9c2" />

* Iceberg: delete via table operation
<img width="1125" height="706" alt="image" src="https://github.com/user-attachments/assets/721611bb-24e9-473d-a143-8f28c7601890" />

* `Kyle Wiley` ถูกลบสำเร็จทั้งสองแบบ

---

