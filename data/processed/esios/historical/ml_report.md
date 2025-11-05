
# REPORTE DE MACHINE LEARNING - E·SIOS
## Fecha de generación: 2025-10-29 14:22:49

## RESUMEN DE MODELOS


### Random Forest
- **R² Score (entrenamiento)**: 0.9952
- **R² Score (prueba)**: 0.9805
- **RMSE (entrenamiento)**: 135.10
- **RMSE (prueba)**: 144.61
- **MAE (entrenamiento)**: 106.87
- **MAE (prueba)**: 121.88


### Linear Regression
- **R² Score (entrenamiento)**: 1.0000
- **R² Score (prueba)**: 0.9812
- **RMSE (entrenamiento)**: 0.00
- **RMSE (prueba)**: 141.75
- **MAE (entrenamiento)**: 0.00
- **MAE (prueba)**: 124.18


## CARACTERÍSTICAS MÁS IMPORTANTES

- **is_weekend**: 0.1215
- **dayofweek**: 0.0794
- **demand_1293_h23**: 0.0761
- **demand_1293_h08**: 0.0571
- **demand_1293_h21**: 0.0566
- **total_demand**: 0.0481
- **demand_1293_h17**: 0.0480
- **demand_1293_h01**: 0.0378
- **demand_1293_h07**: 0.0378
- **demand_1293_h15**: 0.0374

## ARCHIVOS GENERADOS
- `model_comparison.png`: Comparación de modelos
- `predictions_vs_actual.png`: Predicciones vs valores reales
- `ml_report.md`: Este reporte

## PRÓXIMOS PASOS RECOMENDADOS
1. Recolectar más datos históricos para mejorar el modelo
2. Probar otros algoritmos (XGBoost, LSTM, etc.)
3. Implementar validación cruzada temporal
4. Crear pipeline de predicción en tiempo real
