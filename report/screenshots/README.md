# Screenshots Folder

This folder now contains the generated fullscreen screenshots required by the report.

Files:

1. `01_docker_compose_up_fullscreen.png`
2. `02_data_preparation_100_docs_fullscreen.png`
3. `03_index_creation_success_fullscreen.png`
4. `04_hdfs_indexer_layout_fullscreen.png`
5. `05_cassandra_load_success_fullscreen.png`
6. `06_query_history_money_fullscreen.png`
7. `07_query_second_fullscreen.png`
8. `08_query_third_fullscreen.png`

All names are referenced in `../report.md`.

Regeneration command:

```powershell
powershell -ExecutionPolicy Bypass -File report/screenshots/generate_screenshots.ps1
```
