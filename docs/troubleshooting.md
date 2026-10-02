# 🔧 Troubleshooting Log

Every command in this pipeline was built and verified in the shell before being moved into Airflow. This log documents the real issues found along the way: the **symptom**, how it was **diagnosed**, and the **fix**. Most of them fail silently, which is exactly why they're worth writing down.

---

## Part 1 — Shell pipeline

### 1. Script "stuck" with no output

- **Symptom:** `cut` never finished.
- **Diagnosis:** the path `/vehicle-data.csv` pointed at the filesystem root, not the working folder. When `cut` doesn't get a readable file argument it waits for standard input, so it looked frozen.
- **Fix:** relative path `./vehicle-data.csv`.
- **Lesson:** a "stuck" command is often *waiting*, not *looping*.

### 2. Row count mismatch: 9,999 vs 10,000

- **Symptom:** `wc -l` gave 9,999 for the source and 10,000 for the extract.
- **Diagnosis:** `wc -l` counts **newline characters**, not rows. `tail -c 1 | od -c` showed the source ends with `5` (data), not `\n`.
- **Fix:** none needed. `cut` writes `\n` after every line, so the output is properly terminated. Both files have 10,000 rows.
- **Lesson:** when counts disagree, check how the counting tool works before assuming data loss.

### 3. Hidden carriage returns in the TSV

- **Symptom:** none visible, which is the danger.
- **Diagnosis:** `cat -A` showed `^M$` at the end of every line (Windows CRLF). The last extracted field would be `PC7C042B7\r`.
- **Fix:** `tr -d '\r'` in the pipeline.
- **Verification:** `cat -A tsv_data.csv | head` shows `$` only.

### 4. Fixed-width file breaks `cut -d ' '`

- **Symptom:** wrong columns on some rows.
- **Diagnosis:** padding spaces create a variable number of empty fields between values, so field numbers shift from row to row.
- **Fix:** select by character position: `cut -c 59-`. The open-ended range avoids hard-coding the line length.
- **Verification:** `sort | uniq -c` over all rows showed only 9 valid combinations.

### 5. Header written to file, but merged data missing

- **Symptom:** the screen preview looked right, but the file contained only the header.
- **Diagnosis:** in `(echo "..." > file) | paste ...`, only `echo` was inside the group and it redirected straight to the file; `paste` ran outside, and its output went to the screen.
- **Fix:** group both commands, separated by `;`, with one redirection: `(echo "..."; paste ...) > file`.
- **Lesson:** always verify the output **file**, not the preview.

### 6. Uppercasing the vehicle type

| Attempt | Problem |
|---|---|
| `tr 'a-z' 'A-Z'` on whole lines | Works on characters, not fields; uppercased timestamps too |
| `awk 'NR==2, NR==10000 {...}'` | Hard-coded range dropped the last row |
| `awk '{print toupper($4)}'` | Printed only one column |
| `awk '{toupper($4); print}'` | `toupper` **returns** a value; without assignment it is discarded |
| `awk 'NR=1{print} ...'` | `=` assigns instead of compares; every line matched rule 1, so the transform never ran |
| ✅ `awk -F',' '{$4=toupper($4); print}' OFS=','` | Assigning to `$4` rebuilds the line using `OFS` |

---

## Part 2 — Moving into Airflow

### 7. Quotes inside quotes → `SyntaxError`

- **Symptom:** the DAG file couldn't be parsed.
- **Diagnosis:** Python strings in single quotes contained bash single quotes, which ended the Python string early.
- **Fix:** outer Python **double** quotes, inner bash **single** quotes unchanged.
- **Why not swap the inner quotes to double?** Inside bash double quotes, `$4` is expanded by bash (to empty) before awk sees it, so `{$4=toupper($4)}` silently becomes `{=toupper()}`.

### 8. Files not found from Airflow tasks

- **Symptom:** tasks couldn't find inputs, or downstream tasks couldn't find upstream outputs.
- **Diagnosis:** Airflow runs each `BashOperator` in its own temporary directory, so relative paths point somewhere different for every task.
- **Fix:** every task starts with `cd <staging> &&`, a single shared working folder. The `&&` acts as a gate: if `cd` fails, nothing runs in the wrong place.
- **Related slip:** `./home/project/...` is relative. An absolute path starts with `/`.

### 9. `ModuleNotFoundError: No module named 'airflow'`

- **Diagnosis:** `/usr/bin/python3` was a different interpreter from the one Airflow uses (`head -1 $(which airflow)` shows it).
- **Fix:** validate the DAG with Airflow itself: `airflow dags list-import-errors`.

### 10. `No module named 'airflow.sdk'`

- **Diagnosis:** `pip` showed Airflow 3.3.2, but the server parsing DAGs was Airflow 2, where `airflow.sdk` doesn't exist. Meanwhile, the original Airflow 2 imports (`bash_operator`, `days_ago`) are removed in Airflow 3.
- **Fix:** version-tolerant imports:
  ```python
  try:
      from airflow.sdk import DAG
      from airflow.providers.standard.operators.bash import BashOperator
  except ImportError:
      from airflow import DAG
      from airflow.operators.bash import BashOperator
  ```
  plus a fixed `start_date=datetime(...)` and `catchup=False` instead of `days_ago`.

### 11. `tar: ./tolldata.tgz: Cannot open: No such file or directory`

- **Diagnosis:** the error came from `tar`, so the `cd` had succeeded. The archive was one folder **above** `staging`.
- **Fix:** `tar -xf ../tolldata.tgz`. The archive is read from the parent folder and extracted into `staging`.
- **Bonus:** `retries=1` with a 5-minute delay let the failed run recover automatically once the fix was in place (the long first bar in the grid screenshot).

---

## Debugging toolkit

| Tool | Use |
|---|---|
| `cat -A file \| head` | Reveal tabs (`^I`), carriage returns (`^M`), line ends (`$`) |
| `tail -c 1 file \| od -c` | Inspect the last byte (trailing newline?) |
| `wc -l` / `wc -c` | Count newlines / bytes |
| `sort \| uniq -c` | Profile the distinct values of a column |
| `awk -F',' '{print NF}' file \| sort \| uniq -c` | Check every row has the same number of fields |
| `airflow dags list-import-errors` | See why a DAG doesn't load |
| `airflow dags test <dag_id>` | Run a DAG once in the terminal with full logs |
