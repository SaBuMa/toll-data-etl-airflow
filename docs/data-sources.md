# 📂 Data Sources

All three files come packed in `tolldata.tgz`. Each one represents a different toll operator with its own IT setup. Before extracting anything, every file was inspected at the byte level with `cat -A`, which makes invisible characters visible (`^I` = tab, `^M` = carriage return, `$` = end of line).

---

## 1. `vehicle-data.csv` — comma-separated

```
1,Thu Aug 19 21:54:38 2021,125094,car,2,VC965
```

| Field | Content | Extracted |
|---|---|---|
| 1 | Rowid | ✅ |
| 2 | Timestamp | ✅ |
| 3 | Anonymized Vehicle number | ✅ |
| 4 | Vehicle type | ✅ |
| 5+ | Other attributes | — |

**Command:** `cut -d ',' -f 1-4 vehicle-data.csv > csv_data.csv`

**Quirk:** the file has **no newline after the last row**, so `wc -l` reports 9,999 for 10,000 rows. `cut` terminates every output line with `\n`, so the extract is normalized automatically.

```bash
tail -c 1 vehicle-data.csv | od -c     # 0000000   5      ← last byte is data, not \n
tail -c 1 csv_data.csv | od -c         # 0000000  \n
```

---

## 2. `tollplaza-data.tsv` — tab-separated, Windows line endings

```
1^IThu Aug 19 21:54:38 2021^I125094^Icar^I2^I4856^IPC7C042B7^M$
```

| Field | Content | Extracted |
|---|---|---|
| 1–4 | Rowid, Timestamp, Vehicle number, Vehicle type | — (duplicated in CSV) |
| 5 | Number of axles | ✅ |
| 6 | Tollplaza id | ✅ |
| 7 | Tollplaza code | ✅ |

**Command:** `cut -f 5-7 tollplaza-data.tsv | tr -d '\r' | tr '\t' ',' > tsv_data.csv`

**Quirks:**
- Tab is `cut`'s default delimiter, so no `-d` is needed.
- `^M$` at the end of every line means **CRLF** (`\r\n`) line endings, likely exported from Windows. Because field 7 touches the end of the line, it would carry the hidden `\r` (`PC7C042B7\r`), breaking joins, filters and comparisons. `tr -d '\r'` removes it.

---

## 3. `payment-data.txt` — fixed-width

```
     1 Thu Aug 19 21:54:38 2021 125094     4856 PC7C042B7 PTE VC965$
     3 Sat Aug 14 17:19:04 2021 8538286    4070 PCEECA8B2 PTE VC965$
```

| Characters | Content | Extracted |
|---|---|---|
| 1–58 | Rowid, Timestamp, Vehicle number, Tollplaza id, Tollplaza code | — |
| 59–61 | Type of Payment code | ✅ |
| 63–67 | Vehicle Code | ✅ |

**Command:** `cut -c 59- payment-data.txt | tr ' ' ',' > fixed_width_data.csv`

**Quirk:** values are padded to a fixed width (`125094     ` vs `8538286    `), so splitting on spaces gives a **different field number** for the same column on different rows. Character **positions**, however, never change. Every line is 67 characters long (`wc -c` reports 68, including `\n`).

Verification across all 10,000 rows:

```bash
cut -c 59- payment-data.txt | tr ' ' ',' | sort | uniq -c
```
```
   2095 PTC,VC965      771 PTC,VCB43      415 PTC,VCD2F
   2160 PTE,VC965      819 PTE,VCB43      397 PTE,VCD2F
   2183 PTP,VC965      778 PTP,VCB43      382 PTP,VCD2F
```

Nine clean combinations, totaling 10,000 rows: no truncated values, no shifted positions.

---

## Final schema — `transformed_data.csv`

| # | Column | Source | Example |
|---|---|---|---|
| 1 | Rowid | CSV | `1` |
| 2 | Timestamp | CSV | `Thu Aug 19 21:54:38 2021` |
| 3 | Anonymized Vehicle number | CSV | `125094` |
| 4 | Vehicle type (uppercased) | CSV | `CAR` |
| 5 | Number of axles | TSV | `2` |
| 6 | Tollplaza id | TSV | `4856` |
| 7 | Tollplaza code | TSV | `PC7C042B7` |
| 8 | Type of Payment code | Fixed-width | `PTE` |
| 9 | Vehicle Code | Fixed-width | `VC965` |
