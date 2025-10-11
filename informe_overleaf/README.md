# Informe Técnico - Pipeline Big Data Music Analytics

## 📄 Contenido

Este documento LaTeX contiene el informe completo del proyecto de análisis de datos musicales con Big Data.

## 🎯 Secciones Incluidas

1. **Introducción**: Contexto, objetivos y descripción de datos
2. **Arquitectura del Sistema**: AWS EMR, S3, estructura de datos
3. **Pipeline de Procesamiento**: Diagrama de flujo completo
4. **Implementación de Jobs**:
   - Jobs 1-5: Hive + Spark
   - Job 6: Hive (Top Charts)
   - Jobs 7-10: MapReduce
5. **Análisis de Resultados**: Métricas, costos, insights
6. **Lecciones Aprendidas**: Trade-offs, optimizaciones
7. **Conclusiones**: Logros y trabajo futuro
8. **Apéndices**: Código fuente y referencias

## 📊 Gráficos Incluidos

- **Pipeline de procesamiento** (TikZ)
- **Evolución de características musicales por década** (pgfplots)
- **Distribución de usuarios por actividad** (pgfplots)
- Múltiples tablas con resultados

## 🚀 Cómo Compilar

### Opción 1: Overleaf (Recomendado)

1. Ir a [Overleaf](https://www.overleaf.com)
2. Crear nuevo proyecto → Upload Project
3. Subir `main.tex`
4. Compilar (se compilará automáticamente)

### Opción 2: Local con LaTeX

```bash
pdflatex main.tex
pdflatex main.tex  # Segunda vez para referencias
```

## 📦 Dependencias LaTeX

El documento usa los siguientes paquetes:
- `graphicx` - Imágenes
- `tikz` - Diagramas
- `pgfplots` - Gráficos estadísticos
- `listings` - Código con syntax highlighting
- `hyperref` - Enlaces
- `booktabs` - Tablas profesionales
- `babel[spanish]` - Soporte español

Todos están disponibles en distribuciones modernas de LaTeX (TeXLive, MiKTeX).

## 📈 Estadísticas del Documento

- **Páginas**: ~25-30 páginas
- **Tablas**: 6 tablas de datos
- **Gráficos**: 3 visualizaciones
- **Código**: 10+ bloques de código comentado
- **Referencias**: 4 fuentes técnicas

## 🎨 Estilo

- Formato: A4, 12pt
- Márgenes: 2.5cm
- Código: Syntax highlighting con colores
- Tablas: Estilo booktabs profesional
- Secciones numeradas con tabla de contenidos

## 📝 Notas

- El informe está en español
- Incluye código real de los jobs implementados
- Datos y métricas son reales del proyecto
- Gráficos generados con datos procesados

## 🔄 Actualizaciones Futuras

Si necesitas modificar:
- Agregar más gráficos: Usa `pgfplots` o incluye imágenes PNG
- Modificar datos: Edita las tablas y coordenadas de gráficos
- Agregar secciones: Mantén la estructura numerada
- Cambiar estilo de código: Modifica `\lstdefinestyle{mystyle}`
