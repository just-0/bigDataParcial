import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import os

# Configuración de la página
st.set_page_config(
    page_title="MAESTRO Music Analytics",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título principal
st.title("🎵 MAESTRO Music Analytics Dashboard")
st.markdown("*Análisis de 50K+ canciones y 9.7M+ interacciones de usuarios*")
st.markdown("---")

# Función para cargar datos Parquet de Hive
@st.cache_data
def load_hive_parquet(folder_path):
    """Carga archivos Parquet generados por Hive (sin extensión .parquet)"""
    if not os.path.exists(folder_path):
        return None
    
    # Buscar todos los archivos que NO sean _SUCCESS ni carpetas $folder$
    files = []
    for f in os.listdir(folder_path):
        if f != '_SUCCESS' and not f.endswith('$folder$') and not f.startswith('.'):
            full_path = os.path.join(folder_path, f)
            if os.path.isfile(full_path):
                files.append(full_path)
    
    if not files:
        return None
    
    dfs = []
    for file in files:
        try:
            df = pd.read_parquet(file)
            dfs.append(df)
        except Exception as e:
            st.warning(f"Error cargando {file}: {e}")
    
    return pd.concat(dfs, ignore_index=True) if dfs else None

# Sidebar - Navegación
st.sidebar.title("📊 Navegación")
page = st.sidebar.radio(
    "Selecciona una vista:",
    [
        "🏠 Resumen General",
        "📈 Análisis Exploratorio (Job 3)",
        "📉 Tendencias (Job 4)",
        "🤖 Recomendaciones ALS (Job 5)",
        "📊 Datos Limpios (Job 2)"
    ]
)

st.sidebar.markdown("---")
st.sidebar.info("""
**Pipeline MAESTRO**
- Job 1: Ingesta ✅
- Job 2: Limpieza ✅
- Job 3: Análisis ✅
- Job 4: Tendencias ✅
- Job 5: Recomendador ✅
""")


# ============================================================================
# PÁGINA 1: RESUMEN GENERAL
# ============================================================================
if page == "🏠 Resumen General":
    st.header("🏠 Resumen General del Dataset")
    
    # Cargar datos principales
    music_stats = load_hive_parquet("data/cleaned/music_with_stats")
    
    if music_stats is not None:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Canciones", f"{len(music_stats):,}")
        with col2:
            total_plays = music_stats['total_plays'].sum()
            st.metric("Total Reproducciones", f"{int(total_plays):,}")
        with col3:
            total_listeners = music_stats['unique_listeners'].sum()
            st.metric("Total Oyentes", f"{int(total_listeners):,}")
        with col4:
            avg_popularity = music_stats['popularity_score'].mean()
            st.metric("Popularidad Promedio", f"{avg_popularity:,.0f}")
        
        st.markdown("---")
        
        # Top 10 canciones más populares
        st.subheader("🔥 Top 10 Canciones Más Populares")
        top_songs = music_stats.nlargest(10, 'popularity_score')[
            ['title', 'artist', 'total_plays', 'unique_listeners', 'popularity_score']
        ]
        st.dataframe(top_songs, use_container_width=True)
        
        # Distribución de géneros
        st.subheader("🎸 Distribución de Géneros")
        genre_counts = music_stats['genre'].value_counts().head(15)
        fig = px.bar(
            x=genre_counts.values,
            y=genre_counts.index,
            orientation='h',
            labels={'x': 'Número de Canciones', 'y': 'Género'},
            title="Top 15 Géneros Musicales"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Distribución por década
        st.subheader("📅 Distribución por Década")
        music_stats['decade'] = (music_stats['year'] // 10) * 10
        decade_counts = music_stats[music_stats['year'] > 0]['decade'].value_counts().sort_index()
        fig = px.line(
            x=decade_counts.index,
            y=decade_counts.values,
            labels={'x': 'Década', 'y': 'Número de Canciones'},
            title="Canciones por Década",
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.error("No se pudieron cargar los datos. Verifica que existan archivos en data/cleaned/music_with_stats/")


# ============================================================================
# PÁGINA 2: ANÁLISIS EXPLORATORIO (JOB 3)
# ============================================================================
elif page == "📈 Análisis Exploratorio (Job 3)":
    st.header("📈 Análisis Exploratorio de Datos")
    
    # Intentar cargar las tablas disponibles
    available_tables = []
    analytics_dir = "data/analytics"
    
    if os.path.exists(analytics_dir):
        for item in os.listdir(analytics_dir):
            if not item.endswith('$folder$'):
                table_path = os.path.join(analytics_dir, item)
                if os.path.isdir(table_path):
                    available_tables.append(item)
    
    if available_tables:
        st.success(f"Tablas disponibles: {', '.join(available_tables)}")
        
        # Cargar y mostrar cada tabla disponible
        for table in available_tables:
            with st.expander(f"📊 {table.replace('_', ' ').title()}"):
                df = load_hive_parquet(f"{analytics_dir}/{table}")
                if df is not None:
                    st.dataframe(df.head(20), use_container_width=True)
                    
                    # Gráfico si hay columnas numéricas
                    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                    if len(numeric_cols) > 0 and len(df) > 0:
                        st.bar_chart(df.head(15).set_index(df.columns[0])[numeric_cols[0]])
                else:
                    st.warning(f"No se pudo cargar {table}")
    else:
        st.warning("No se encontraron tablas de análisis en data/analytics/")


# ============================================================================
# PÁGINA 3: TENDENCIAS (JOB 4)
# ============================================================================
elif page == "📉 Tendencias (Job 4)":
    st.header("📉 Descubrimiento de Tendencias Musicales")
    
    # Listar tablas disponibles
    available_tables = []
    trends_dir = "data/trends"
    
    if os.path.exists(trends_dir):
        for item in os.listdir(trends_dir):
            if not item.endswith('$folder$'):
                table_path = os.path.join(trends_dir, item)
                if os.path.isdir(table_path):
                    available_tables.append(item)
    
    if available_tables:
        st.success(f"Tablas disponibles: {', '.join(available_tables)}")
        
        # Selector de tabla
        selected_table = st.selectbox("Selecciona una tabla:", available_tables)
        
        df = load_hive_parquet(f"{trends_dir}/{selected_table}")
        if df is not None:
            st.subheader(f"📊 {selected_table.replace('_', ' ').title()}")
            st.dataframe(df, use_container_width=True)
            
            # Intentar crear visualización automática
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            if len(numeric_cols) >= 2:
                col1, col2 = st.columns(2)
                with col1:
                    st.metric(f"Total registros", len(df))
                with col2:
                    st.metric(f"Promedio {numeric_cols[0]}", f"{df[numeric_cols[0]].mean():.2f}")
        else:
            st.warning(f"No se pudo cargar {selected_table}")
    else:
        st.warning("No se encontraron tablas de tendencias en data/trends/")


# ============================================================================
# PÁGINA 4: RECOMENDACIONES ALS (JOB 5)
# ============================================================================
elif page == "🤖 Recomendaciones ALS (Job 5)":
    st.header("🤖 Sistema de Recomendación con ALS")
    
    # Cargar métricas del modelo
    metrics = load_hive_parquet("data/metrics/model_evaluation")
    
    if metrics is not None:
        st.subheader("📊 Métricas del Modelo ALS")
        st.dataframe(metrics, use_container_width=True)
    else:
        st.warning("Métricas del modelo no disponibles")
    
    st.markdown("---")
    
    # Listar carpetas de recomendaciones disponibles
    recs_dir = "data/recommendations"
    available_recs = []
    
    if os.path.exists(recs_dir):
        for item in os.listdir(recs_dir):
            if not item.endswith('$folder$'):
                table_path = os.path.join(recs_dir, item)
                if os.path.isdir(table_path):
                    available_recs.append(item)
    
    if available_recs:
        st.success(f"Tablas de recomendaciones disponibles: {', '.join(available_recs)}")
        
        selected_rec = st.selectbox("Selecciona tipo de recomendación:", available_recs)
        
        df = load_hive_parquet(f"{recs_dir}/{selected_rec}")
        if df is not None:
            st.subheader(f"📊 {selected_rec.replace('_', ' ').title()}")
            
            # Mostrar info de las columnas
            st.write(f"**Columnas disponibles:** {', '.join(df.columns.tolist())}")
            st.write(f"**Total de registros:** {len(df):,}")
            
            # Mostrar primeros registros
            st.dataframe(df.head(50), use_container_width=True)
            
            # Descargar
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label=f"📥 Descargar {selected_rec}",
                data=csv,
                file_name=f'{selected_rec}.csv',
                mime='text/csv'
            )
        else:
            st.warning(f"No se pudo cargar {selected_rec}")
    else:
        st.warning("No se encontraron recomendaciones en data/recommendations/")


# ============================================================================
# PÁGINA 5: DATOS LIMPIOS (JOB 2)
# ============================================================================
elif page == "📊 Datos Limpios (Job 2)":
    st.header("📊 Exploración de Datos Limpios")
    
    music_stats = load_hive_parquet("data/cleaned/music_with_stats")
    
    if music_stats is not None:
        st.subheader("🔍 Explorar Dataset")
        
        # Mostrar info del dataset
        st.write(f"**Total de canciones:** {len(music_stats):,}")
        st.write(f"**Columnas:** {', '.join(music_stats.columns.tolist())}")
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        with col1:
            genres = ['Todos'] + sorted(music_stats['genre'].unique().tolist())
            selected_genre = st.selectbox("Filtrar por género:", genres)
        with col2:
            years_valid = music_stats[music_stats['year'] > 0]['year']
            if len(years_valid) > 0:
                min_year = int(years_valid.min())
                max_year = int(years_valid.max())
                year_range = st.slider("Rango de años:", min_year, max_year, (min_year, max_year))
            else:
                year_range = (1900, 2025)
        with col3:
            min_plays = st.number_input("Mínimo de reproducciones:", min_value=0, value=0)
        
        # Aplicar filtros
        filtered_df = music_stats.copy()
        if selected_genre != 'Todos':
            filtered_df = filtered_df[filtered_df['genre'] == selected_genre]
        filtered_df = filtered_df[
            (filtered_df['year'] >= year_range[0]) & 
            (filtered_df['year'] <= year_range[1]) &
            (filtered_df['total_plays'] >= min_plays)
        ]
        
        st.write(f"**{len(filtered_df):,} canciones encontradas**")
        
        # Mostrar datos
        display_cols = ['title', 'artist', 'genre', 'year', 'total_plays', 'unique_listeners']
        display_cols = [col for col in display_cols if col in filtered_df.columns]
        
        st.dataframe(
            filtered_df[display_cols].head(100),
            use_container_width=True
        )
        
        # Descargar datos filtrados
        csv = filtered_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Descargar datos filtrados (CSV)",
            data=csv,
            file_name='music_data_filtered.csv',
            mime='text/csv'
        )
    else:
        st.error("No se pudieron cargar los datos")


# Footer
st.markdown("---")
st.markdown("*Dashboard creado con Streamlit | Proyecto MAESTRO Music Analytics*")
