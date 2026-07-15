# 📁 Directorio de Datos (ml/data/)

Este directorio almacena los datasets utilizados para entrenar y evaluar los modelos de Machine Learning (GBDT). 

Debido a su tamaño, **los archivos `.csv` están excluidos del control de versiones en git** (configurado en el `.gitignore`).

### ⚠️ Requisito previo para ejecutar los Notebooks

Para poder ejecutar los notebooks (`01_preprocessing...` y `02_training...`) o la aplicación de Streamlit, **debes crear o asegurarte de que existe** esta carpeta y **copiar el archivo original de ingesta aquí**.

1. Obtén el archivo `staging_flat.csv` original (proveniente de la capa Bronze/Ingesta).
2. Pégalo directamente en esta carpeta (`ml/data/`).

La estructura de esta carpeta debe lucir así antes de comenzar:

```text
ml/
└── data/
    ├── README.md
    └── staging_flat.csv   <-- ¡Debes colocar este archivo aquí manualmente!
```

Una vez que coloques el archivo, puedes proceder a ejecutar el **Notebook 01** para generar los archivos procesados (`processed_features.csv`, etc.) que también se guardarán temporalmente en esta carpeta.
