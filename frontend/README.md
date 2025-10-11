# Music Data Analytics - Frontend

Dashboard HTML para visualizar los resultados de los jobs de Big Data.

## 📋 Prerequisitos

- AWS CLI configurado con perfil `dev`
- Python 3 con pandas y pyarrow instalados:
  ```bash
  pip install pandas pyarrow
  ```

## 🚀 Uso Rápido

1. **Descargar datos y abrir dashboard:**
   ```bash
   download_and_run.bat
   ```

   Esto automáticamente:
   - Descarga todos los resultados de S3
   - Convierte Parquet y TSV a JSON
   - Abre el dashboard en tu navegador

2. **Abrir el dashboard (si ya descargaste los datos):**
   - Doble click en `index.html`

## 📊 Datos Mostrados

### Job 6 - Top Charts
- Top 20 canciones con metadata (sample 1%)
- Título, artista, género, plays, listeners

### Job 7 - Artist Statistics
- Total de plays por artista
- Top 10 artistas más escuchados
- Estadísticas generales

### Job 8 - User Genres
- Top 3 géneros por usuario
- Preferencias musicales

### Job 9 - Decade Statistics
- Evolución de características musicales por década
- Danceability, Energy, Tempo, Valence

### Job 10 - User Activity
- Clasificación de usuarios:
  - Casual (< 5 canciones)
  - Regular (5-19 canciones)
  - Active (20-49 canciones)
  - Heavy (50-99 canciones)
  - Power User (100+ canciones)

## 📁 Estructura

```
frontend/
├── index.html                    # Dashboard principal
├── download_and_run.bat          # Script Windows todo-en-uno
├── convert_parquet_to_json.py    # Convertidor de datos
├── data/                         # Datos descargados (git-ignored)
│   ├── charts/
│   ├── job7_artist_plays/
│   ├── job8_user_genres/
│   ├── job9_decade_stats/
│   ├── job10_user_activity/
│   └── json/                     # JSONs convertidos
└── README.md
```

## 🔧 Scripts Individuales

Si quieres más control:

1. **Solo descargar datos:**
   ```bash
   aws s3 sync s3://emr-logs-1758750407/music-data/charts/top_20_sample/ data/charts/top_20_sample/ --profile dev
   # etc...
   ```

2. **Solo convertir a JSON:**
   ```bash
   python convert_parquet_to_json.py
   ```

3. **Solo abrir dashboard:**
   - Doble click en `index.html`

## 🎨 Features

- **Tabs interactivos** para cada job
- **Tablas responsive** con hover effects
- **Stats cards** con métricas resumen
- **Dark theme** para mejor visualización
- **Sin dependencias externas** (standalone HTML)

## ⚠️ Notas

- Los datos de Jobs 7-10 son archivos TSV (MapReduce output)
- Job 6 usa solo 1% sample de datos (para que sea rápido)
- Para ver datos completos, ejecuta Job 6 con 100% de datos
