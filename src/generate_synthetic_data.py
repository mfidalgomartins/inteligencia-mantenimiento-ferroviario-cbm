from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from src.config import DATA_RAW_DIR, RANDOM_SEED


HISTORY_START = "2024-01-01"
HISTORY_END = "2025-12-31"


@dataclass
class SyntheticRailwayData:
    flotas: pd.DataFrame
    unidades: pd.DataFrame
    depositos: pd.DataFrame
    componentes_criticos: pd.DataFrame
    sensores_componentes: pd.DataFrame
    inspecciones_automaticas: pd.DataFrame
    eventos_mantenimiento: pd.DataFrame
    fallas_historicas: pd.DataFrame
    alertas_operativas: pd.DataFrame
    intervenciones_programadas: pd.DataFrame
    disponibilidad_servicio: pd.DataFrame
    asignacion_servicio: pd.DataFrame
    backlog_mantenimiento: pd.DataFrame
    parametros_operativos_contexto: pd.DataFrame
    escenarios_mantenimiento: pd.DataFrame


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-x))


def _build_flotas() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("FLOTA01", "Cercanias Norte", "EMU", "Operador Iberia Rail", "Norte", 2012, 0.88, 4.4, "mixta"),
            ("FLOTA02", "Intercity Este", "EMU", "Operador Iberia Rail", "Este", 2015, 0.82, 4.1, "preventiva"),
            ("FLOTA03", "Regional Centro", "DMU", "Operador Iberia Rail", "Centro", 2010, 0.86, 4.3, "mixta"),
            ("FLOTA04", "Alta Demanda Sur", "EMU", "Operador Iberia Rail", "Sur", 2018, 0.93, 4.7, "basada_condicion"),
            ("FLOTA05", "Tram-Train Mediterraneo", "LRV", "Operador Iberia Rail", "Este", 2016, 0.79, 3.8, "preventiva"),
            ("FLOTA06", "Suburbana Atlantica", "EMU", "Operador Iberia Rail", "Norte", 2014, 0.84, 4.0, "mixta"),
        ],
        columns=[
            "flota_id",
            "nombre_flota",
            "tipo_material",
            "operador",
            "region",
            "ano_fabricacion_base",
            "uso_intensidad",
            "criticidad_operativa",
            "estrategia_mantenimiento_actual",
        ],
    )


def _build_depositos() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("DEP01", "Madrid Sur", "Centro", 92, 56, "traccion_y_bogie", 0.89),
            ("DEP02", "Barcelona Litoral", "Este", 88, 52, "pantografo_y_freno", 0.91),
            ("DEP03", "Valencia Puerto", "Este", 72, 44, "wheel_bogie", 0.85),
            ("DEP04", "Sevilla Tecnico", "Sur", 66, 41, "freno_y_puertas", 0.82),
            ("DEP05", "Bilbao Norte", "Norte", 61, 36, "traccion", 0.83),
            ("DEP06", "Zaragoza Hub", "Centro", 58, 34, "pantografo", 0.78),
            ("DEP07", "Malaga Costa", "Sur", 54, 30, "bogie", 0.76),
            ("DEP08", "A Coruna Atlantico", "Norte", 48, 27, "wheel_brake", 0.74),
            ("DEP09", "Valladolid Centro", "Centro", 52, 31, "freno", 0.77),
            ("DEP10", "Murcia Levante", "Este", 46, 26, "puertas_hvac", 0.73),
        ],
        columns=[
            "deposito_id",
            "nombre_deposito",
            "region",
            "capacidad_taller",
            "capacidad_inspeccion",
            "especializacion_tecnica",
            "carga_operativa_media",
        ],
    )


def _build_lineas() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("L01", "Norte", "urbana"),
            ("L02", "Norte", "suburbana"),
            ("L03", "Norte", "regional"),
            ("L04", "Centro", "urbana"),
            ("L05", "Centro", "suburbana"),
            ("L06", "Centro", "regional"),
            ("L07", "Este", "urbana"),
            ("L08", "Este", "suburbana"),
            ("L09", "Este", "intercity"),
            ("L10", "Este", "regional"),
            ("L11", "Sur", "urbana"),
            ("L12", "Sur", "regional"),
            ("L13", "Sur", "intercity"),
            ("L14", "Norte", "intercity"),
            ("L15", "Centro", "intercity"),
            ("L16", "Este", "tranvia"),
        ],
        columns=["linea_servicio", "region", "tipo_explotacion"],
    )


def _generate_unidades(
    flotas: pd.DataFrame,
    depositos: pd.DataFrame,
    lineas: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    base_units = {
        "FLOTA01": 26,
        "FLOTA02": 22,
        "FLOTA03": 23,
        "FLOTA04": 28,
        "FLOTA05": 20,
        "FLOTA06": 25,
    }
    config_by_type = {
        "EMU": ["4_coches", "5_coches", "6_coches"],
        "DMU": ["3_coches", "4_coches"],
        "LRV": ["2_modulos", "3_modulos"],
    }

    rows = []
    unit_idx = 1
    dep_region_map = depositos.set_index("deposito_id")["region"].to_dict()

    for flota in flotas.itertuples(index=False):
        candidate_deps = [dep for dep, reg in dep_region_map.items() if reg == flota.region]
        lineas_region = lineas[lineas["region"] == flota.region]["linea_servicio"].tolist()
        n_units = base_units[flota.flota_id]

        for _ in range(n_units):
            unidad_id = f"UNI{unit_idx:04d}"
            unit_idx += 1

            deposito_id = rng.choice(candidate_deps)
            linea = rng.choice(lineas_region)
            years_in_service = rng.uniform(1.2, 14.0)
            entry_date = pd.Timestamp(HISTORY_START) - pd.to_timedelta(int(years_in_service * 365), unit="D")

            km_factor = {
                "EMU": 185_000,
                "DMU": 150_000,
                "LRV": 110_000,
            }[flota.tipo_material]
            km_acc = int(km_factor * years_in_service * rng.uniform(0.72, 1.28) * (0.9 + 0.25 * flota.uso_intensidad))
            hours_acc = int((km_acc / rng.uniform(38, 72)) * rng.uniform(0.9, 1.15))

            rows.append(
                (
                    unidad_id,
                    flota.flota_id,
                    deposito_id,
                    linea,
                    entry_date.date().isoformat(),
                    km_acc,
                    hours_acc,
                    rng.choice(config_by_type[flota.tipo_material]),
                    round(float(np.clip(rng.normal(flota.criticidad_operativa, 0.4), 2.5, 5.0)), 2),
                    round(float(np.clip(rng.normal(0.945, 0.02), 0.89, 0.985)), 3),
                )
            )

    return pd.DataFrame(
        rows,
        columns=[
            "unidad_id",
            "flota_id",
            "deposito_id",
            "linea_servicio",
            "fecha_entrada_servicio",
            "kilometraje_acumulado_km",
            "horas_operacion_acumuladas",
            "configuracion_unidad",
            "criticidad_servicio",
            "disponibilidad_objetivo",
        ],
    )


def _build_component_catalog() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("TRACCION", "motor", "motor_traction", "wheel", 4.8, 2200, 520000, 0.0010, 1.25, 1.35),
            ("TRACCION", "gearbox", "gearbox_drive", "bogie", 4.5, 1900, 420000, 0.0012, 1.35, 1.05),
            ("RODADURA", "wheelset", "wheel_profile", "wheel", 4.6, 1400, 310000, 0.0016, 1.60, 0.80),
            ("FRENADO", "brake", "brake_disc_pad", "brake", 4.9, 980, 180000, 0.0021, 1.45, 0.85),
            ("SUSPENSION", "bogie", "bogie_frame", "bogie", 4.4, 2400, 450000, 0.0010, 1.20, 1.05),
            ("CAPTACION", "pantograph", "pantograph_head", "pantograph", 4.3, 1150, 230000, 0.0019, 1.30, 1.20),
            ("PUERTAS", "door", "door_actuator", "bogie", 3.5, 1200, 260000, 0.0014, 0.95, 1.40),
            ("AUXILIARES", "hvac", "hvac_compressor", "bogie", 3.2, 1600, 280000, 0.0011, 1.00, 1.10),
        ],
        columns=[
            "sistema_principal",
            "subsistema",
            "tipo_componente",
            "familia_inspeccion",
            "criticidad_componente",
            "vida_util_teorica_dias",
            "vida_util_teorica_ciclos",
            "base_degradacion_diaria",
            "sensibilidad_vibracion",
            "sensibilidad_corriente",
        ],
    )


def _generate_componentes_criticos(
    unidades: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    catalog = _build_component_catalog()
    fabricantes = ["Fabricante_A", "Fabricante_B", "Fabricante_C", "Fabricante_D"]

    rows = []
    component_idx = 1
    history_end = pd.Timestamp(HISTORY_END)

    for unidad in unidades.itertuples(index=False):
        for comp in catalog.itertuples(index=False):
            component_id = f"COMP{component_idx:06d}"
            component_idx += 1

            install_date = history_end - pd.to_timedelta(int(rng.uniform(120, 2000)), unit="D")
            age_days = int((history_end - install_date).days)

            ciclos = int(
                (unidad.horas_operacion_acumuladas * rng.uniform(1.1, 2.6))
                * (0.7 + 0.5 * rng.uniform(0.6, 1.3))
            )

            rows.append(
                (
                    component_id,
                    unidad.unidad_id,
                    comp.sistema_principal,
                    comp.subsistema,
                    comp.tipo_componente,
                    rng.choice(fabricantes),
                    install_date.date().isoformat(),
                    age_days,
                    ciclos,
                    round(float(comp.criticidad_componente + rng.normal(0, 0.25)), 2),
                    int(comp.vida_util_teorica_dias),
                    int(comp.vida_util_teorica_ciclos),
                    comp.familia_inspeccion,
                    float(comp.base_degradacion_diaria),
                    float(comp.sensibilidad_vibracion),
                    float(comp.sensibilidad_corriente),
                    int(rng.choice([0, 1], p=[0.88, 0.12])),
                )
            )

    return pd.DataFrame(
        rows,
        columns=[
            "componente_id",
            "unidad_id",
            "sistema_principal",
            "subsistema",
            "tipo_componente",
            "fabricante_proxy",
            "fecha_instalacion",
            "edad_componente_dias",
            "ciclos_acumulados",
            "criticidad_componente",
            "vida_util_teorica_dias",
            "vida_util_teorica_ciclos",
            "familia_inspeccion",
            "base_degradacion_diaria",
            "sensibilidad_vibracion",
            "sensibilidad_corriente",
            "componente_repetitivo_base",
        ],
    )


def _generate_parametros_operativos_contexto(
    lineas: pd.DataFrame,
    fechas: pd.DatetimeIndex,
    rng: np.random.Generator,
) -> pd.DataFrame:
    rows = []
    day_idx = np.arange(len(fechas))

    for linea in lineas.itertuples(index=False):
        seasonal_temp = 14 + 10 * np.sin(2 * np.pi * day_idx / 365.0 + rng.uniform(-0.4, 0.4))
        humidity = 58 + 16 * np.sin(2 * np.pi * day_idx / 365.0 + rng.uniform(0.6, 1.1))

        exploit_factor = {
            "urbana": 1.15,
            "suburbana": 1.00,
            "regional": 0.90,
            "intercity": 0.98,
            "tranvia": 1.05,
        }[linea.tipo_explotacion]

        intensity = np.clip(
            exploit_factor
            + 0.08 * np.sin(2 * np.pi * day_idx / 180.0)
            + 0.06 * np.where(fechas.dayofweek.values < 5, 1, -1)
            + rng.normal(0, 0.07, size=len(fechas)),
            0.58,
            1.45,
        )

        congestion = np.clip(
            0.38 + 0.42 * intensity + rng.normal(0, 0.08, size=len(fechas)),
            0.12,
            0.98,
        )

        for i, fecha in enumerate(fechas):
            rows.append(
                (
                    fecha.date().isoformat(),
                    linea.linea_servicio,
                    linea.region,
                    round(float(seasonal_temp[i] + rng.normal(0, 1.1)), 2),
                    round(float(np.clip(humidity[i] + rng.normal(0, 3), 20, 95)), 2),
                    linea.tipo_explotacion,
                    round(float(intensity[i]), 3),
                    round(float(congestion[i]), 3),
                )
            )

    return pd.DataFrame(
        rows,
        columns=[
            "fecha",
            "linea_servicio",
            "region",
            "temperatura_ambiente",
            "humedad",
            "tipo_explotacion",
            "intensidad_servicio",
            "nivel_congestion_operativa_proxy",
        ],
    )


def _build_unit_day_context(
    unidades: pd.DataFrame,
    flotas: pd.DataFrame,
    parametros_contexto: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    flota_info = flotas.set_index("flota_id")

    unit_days = unidades[["unidad_id", "flota_id", "deposito_id", "linea_servicio", "criticidad_servicio"]].merge(
        parametros_contexto,
        on="linea_servicio",
        how="inner",
    )

    uso_intensity = unit_days["flota_id"].map(flota_info["uso_intensidad"].to_dict()).astype(float)
    tipo_material = unit_days["flota_id"].map(flota_info["tipo_material"].to_dict())

    base_hours_by_type = {"EMU": 17.0, "DMU": 15.5, "LRV": 14.0}
    speed_by_type = {"EMU": 76.0, "DMU": 68.0, "LRV": 42.0}

    base_hours = tipo_material.map(base_hours_by_type).astype(float)
    base_speed = tipo_material.map(speed_by_type).astype(float)

    intensity = unit_days["intensidad_servicio"].astype(float)
    congestion = unit_days["nivel_congestion_operativa_proxy"].astype(float)

    horas_planificadas = np.clip(
        base_hours
        * (0.82 + 0.30 * intensity)
        * (0.88 + 0.18 * uso_intensity)
        + rng.normal(0, 0.9, size=len(unit_days)),
        6,
        22,
    )

    carga_operativa = np.clip(
        0.52 + 0.30 * intensity + 0.18 * uso_intensity + 0.12 * congestion + rng.normal(0, 0.05, len(unit_days)),
        0.35,
        1.45,
    )

    velocidad_operativa = np.clip(
        base_speed * (0.72 + 0.28 * intensity) * (0.97 + rng.normal(0, 0.035, len(unit_days))),
        24,
        125,
    )

    servicio_planificado = np.clip((horas_planificadas * (1.9 + 0.32 * intensity)).round().astype(int), 2, None)
    reserva_flag = rng.choice([0, 1], size=len(unit_days), p=[0.90, 0.10]).astype(int)

    unit_days["horas_planificadas"] = np.round(horas_planificadas, 3)
    unit_days["carga_operativa"] = np.round(carga_operativa, 4)
    unit_days["velocidad_operativa"] = np.round(velocidad_operativa, 3)
    unit_days["servicio_planificado"] = servicio_planificado
    unit_days["reserva_flag"] = reserva_flag
    unit_days["uso_intensidad_flota"] = np.round(uso_intensity, 4)

    return unit_days


def _sensor_types_for_subsystem(subsystem: str) -> List[str]:
    sensor_map = {
        "motor": ["temperatura", "corriente", "vibracion"],
        "gearbox": ["vibracion", "temperatura", "ruido"],
        "wheelset": ["vibracion", "desgaste", "temperatura", "ruido"],
        "brake": ["presion", "desgaste", "temperatura", "vibracion"],
        "bogie": ["vibracion", "temperatura", "ruido"],
        "pantograph": ["corriente", "vibracion", "temperatura", "presion"],
        "door": ["corriente", "ruido", "vibracion"],
        "hvac": ["temperatura", "corriente", "presion"],
    }
    return sensor_map.get(subsystem, ["temperatura", "vibracion"])


def _simulate_component_states_and_sensors(
    componentes: pd.DataFrame,
    unit_day_context: pd.DataFrame,
    rng: np.random.Generator,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows_daily = []
    sensor_rows = []

    by_unit = {
        uid: df.sort_values("fecha").reset_index(drop=True)
        for uid, df in unit_day_context.groupby("unidad_id", sort=False)
    }

    for comp in componentes.itertuples(index=False):
        ctx = by_unit[comp.unidad_id]
        n = len(ctx)
        if n == 0:
            continue

        temp_amb = ctx["temperatura_ambiente"].to_numpy(dtype=float)
        humedad = ctx["humedad"].to_numpy(dtype=float)
        intensidad = ctx["intensidad_servicio"].to_numpy(dtype=float)
        congestion = ctx["nivel_congestion_operativa_proxy"].to_numpy(dtype=float)
        velocidad = ctx["velocidad_operativa"].to_numpy(dtype=float)
        carga = ctx["carga_operativa"].to_numpy(dtype=float)

        install_date = pd.Timestamp(comp.fecha_instalacion)
        fechas = pd.to_datetime(ctx["fecha"]).to_numpy()
        active_mask = fechas >= np.datetime64(install_date)

        init_wear = np.clip(comp.edad_componente_dias / max(comp.vida_util_teorica_dias, 1) * rng.uniform(0.72, 1.08), 0.03, 1.1)

        env_factor = 1.0 + 0.08 * np.clip((temp_amb - 18) / 20, -0.5, 1.6) + 0.05 * np.clip((humedad - 60) / 20, -1, 1.5)
        load_factor = 0.92 + 0.55 * carga + 0.20 * congestion
        deg_increment = comp.base_degradacion_diaria * env_factor * load_factor

        shock_prob = np.clip(0.003 + 0.006 * intensidad + 0.003 * comp.componente_repetitivo_base, 0.001, 0.03)
        shocks = (rng.random(n) < shock_prob) * rng.gamma(1.5, 0.013, n)

        wear = np.clip(init_wear + np.cumsum(deg_increment + shocks), 0.01, 1.42)
        wear[~active_mask] = np.nan

        temperatura = 41 + 28 * np.nan_to_num(wear, nan=0.0) + 4.2 * intensidad + 0.30 * temp_amb + rng.normal(0, 1.8, n)
        vibracion = 1.1 + 2.6 * np.nan_to_num(wear, nan=0.0) * comp.sensibilidad_vibracion + 1.4 * carga + rng.normal(0, 0.30, n)

        base_pressure = {
            "brake": 6.7,
            "pantograph": 5.4,
            "hvac": 4.2,
        }.get(comp.subsistema, 3.8)
        presion = base_pressure + 0.7 * intensidad - 1.4 * np.nan_to_num(wear, nan=0.0) + rng.normal(0, 0.18, n)

        desgaste = np.clip(np.nan_to_num(wear, nan=0.0) * 100 + rng.normal(0, 3.2, n), 0, 145)
        corriente = 74 + 88 * carga + 70 * np.nan_to_num(wear, nan=0.0) * comp.sensibilidad_corriente + rng.normal(0, 5.0, n)
        ruido = 44 + 9.5 * np.nan_to_num(wear, nan=0.0) + 0.18 * velocidad + rng.normal(0, 1.6, n)
        ambiente_ext = temp_amb + 0.09 * humedad + rng.normal(0, 1.3, n)

        for i in range(n):
            if not active_mask[i]:
                continue
            rows_daily.append(
                (
                    pd.Timestamp(fechas[i]).date().isoformat(),
                    comp.unidad_id,
                    comp.componente_id,
                    comp.subsistema,
                    comp.familia_inspeccion,
                    float(wear[i]),
                    float(temperatura[i]),
                    float(vibracion[i]),
                    float(presion[i]),
                    float(desgaste[i]),
                    float(corriente[i]),
                    float(ruido[i]),
                    float(velocidad[i]),
                    float(carga[i]),
                    float(ambiente_ext[i]),
                    float(intensidad[i]),
                    int(comp.componente_repetitivo_base),
                    float(comp.criticidad_componente),
                    ctx.iloc[i]["deposito_id"],
                    ctx.iloc[i]["linea_servicio"],
                )
            )

            sensor_types = _sensor_types_for_subsystem(comp.subsistema)
            for sensor_tipo in sensor_types:
                if sensor_tipo == "temperatura":
                    valor = temperatura[i]
                elif sensor_tipo == "vibracion":
                    valor = vibracion[i]
                elif sensor_tipo == "presion":
                    valor = presion[i]
                elif sensor_tipo == "desgaste":
                    valor = desgaste[i]
                elif sensor_tipo == "corriente":
                    valor = corriente[i]
                else:
                    valor = ruido[i]

                sensor_rows.append(
                    (
                        f"{pd.Timestamp(fechas[i]).date().isoformat()} 12:00:00",
                        comp.unidad_id,
                        comp.componente_id,
                        sensor_tipo,
                        round(float(valor), 4),
                        round(float(temperatura[i]), 4),
                        round(float(vibracion[i]), 4),
                        round(float(presion[i]), 4),
                        round(float(desgaste[i]), 4),
                        round(float(corriente[i]), 4),
                        round(float(ruido[i]), 4),
                        round(float(velocidad[i]), 4),
                        round(float(carga[i]), 4),
                        round(float(ambiente_ext[i]), 4),
                    )
                )

    daily_state = pd.DataFrame(
        rows_daily,
        columns=[
            "fecha",
            "unidad_id",
            "componente_id",
            "subsistema",
            "familia_inspeccion",
            "degradacion_acumulada",
            "temperatura_operacion",
            "vibracion_proxy",
            "presion_proxy",
            "desgaste_proxy",
            "corriente_proxy",
            "ruido_proxy",
            "velocidad_operativa",
            "carga_operativa",
            "ambiente_externo_proxy",
            "intensidad_servicio",
            "componente_repetitivo_base",
            "criticidad_componente",
            "deposito_id",
            "linea_servicio",
        ],
    )

    sensores = pd.DataFrame(
        sensor_rows,
        columns=[
            "timestamp",
            "unidad_id",
            "componente_id",
            "sensor_tipo",
            "valor_sensor",
            "temperatura_operacion",
            "vibracion_proxy",
            "presion_proxy",
            "desgaste_proxy",
            "corriente_proxy",
            "ruido_proxy",
            "velocidad_operativa",
            "carga_operativa",
            "ambiente_externo_proxy",
        ],
    )

    return daily_state, sensores


def _generate_fallas_historicas(
    daily_state: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    d = daily_state.copy()

    logit = (
        -8.9
        + 5.1 * d["degradacion_acumulada"].to_numpy()
        + 0.50 * d["criticidad_componente"].to_numpy()
        + 0.95 * d["componente_repetitivo_base"].to_numpy()
        + 0.55 * np.clip(d["carga_operativa"].to_numpy() - 0.95, 0, None)
    )
    prob = np.clip(_sigmoid(logit) * 0.028, 0.00001, 0.08)
    failures = rng.random(len(d)) < prob

    fallas = d.loc[failures, ["fecha", "unidad_id", "componente_id", "subsistema", "degradacion_acumulada", "componente_repetitivo_base"]].copy()

    def _severity(w: float) -> int:
        if w < 0.82:
            return 1
        if w < 0.98:
            return 2
        if w < 1.12:
            return 3
        if w < 1.28:
            return 4
        return 5

    mode_map = {
        "motor": ["sobrecalentamiento_estator", "fatiga_bobinado", "desbalance_rotor"],
        "gearbox": ["desalineacion_engranes", "desgaste_rodamiento", "contaminacion_lubricante"],
        "wheelset": ["aplanamiento_rueda", "desgaste_perfil", "fisura_termica"],
        "brake": ["fatiga_discos", "fading_freno", "degradacion_pastilla"],
        "bogie": ["holgura_suspension", "fatiga_bastidor", "desgaste_amortiguador"],
        "pantograph": ["perdida_contacto", "desgaste_carbon", "arco_electrico"],
        "door": ["bloqueo_actuador", "falla_control_puerta", "desajuste_mecanismo"],
        "hvac": ["falla_compresor", "fuga_refrigerante", "obstruccion_filtro"],
    }

    root_causes = [
        "degradacion_progresiva",
        "sobrecarga_operativa",
        "contaminacion",
        "fatiga_material",
        "mantenimiento_suboptimo",
        "variacion_ambiental_extrema",
    ]

    impact_map = {
        1: "bajo",
        2: "medio",
        3: "alto",
        4: "critico",
        5: "critico",
    }

    rows = []
    for i, row in enumerate(fallas.itertuples(index=False), start=1):
        sev = _severity(float(row.degradacion_acumulada))
        downtime = float(np.clip(rng.gamma(1.8 + sev * 0.5, 1.7), 0.5, 72))
        rows.append(
            (
                f"FALLA{i:07d}",
                row.unidad_id,
                row.componente_id,
                row.fecha,
                rng.choice(mode_map.get(row.subsistema, ["falla_generica"])),
                sev,
                impact_map[sev],
                round(downtime, 3),
                rng.choice(root_causes),
                int(bool(row.componente_repetitivo_base)),
            )
        )

    return pd.DataFrame(
        rows,
        columns=[
            "falla_id",
            "unidad_id",
            "componente_id",
            "fecha_falla",
            "modo_falla",
            "severidad_falla",
            "impacto_en_servicio",
            "tiempo_fuera_servicio_horas",
            "causa_raiz_proxy",
            "repetitiva_flag",
        ],
    )


def _generate_inspecciones_automaticas(
    daily_state: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    required_families = {"wheel", "brake", "bogie", "pantograph"}
    subset = daily_state[daily_state["familia_inspeccion"].isin(required_families)].copy()

    subset["fecha_dt"] = pd.to_datetime(subset["fecha"])
    subset["day_index"] = (subset["fecha_dt"] - subset["fecha_dt"].min()).dt.days

    base_schedule = ((subset["day_index"] + subset["componente_id"].str[-2:].astype(int)) % 14 == 0)
    risk_trigger = (subset["degradacion_acumulada"] > 0.78) & (rng.random(len(subset)) < 0.32)
    inspect_mask = base_schedule | risk_trigger

    insp = subset.loc[inspect_mask].copy()
    insp = insp.sort_values(["componente_id", "fecha_dt"]).reset_index(drop=True)

    score = np.clip(22 + insp["degradacion_acumulada"] * 72 + rng.normal(0, 9, len(insp)), 0, 100)
    defect_prob = np.clip(_sigmoid((score - 55) / 9) * 0.95 + 0.03, 0.02, 0.99)
    defect_flag = rng.random(len(insp)) < defect_prob

    severity = pd.cut(
        score,
        bins=[-1, 35, 55, 75, 1000],
        labels=["baja", "media", "alta", "critica"],
    ).astype(str)

    confidence = np.clip(
        0.62
        + 0.28 * (1 - np.abs(insp["ambiente_externo_proxy"].to_numpy() - 24) / 55)
        + rng.normal(0, 0.05, len(insp)),
        0.45,
        0.99,
    )

    rec_map = {
        "baja": "monitorizar",
        "media": "reinspeccion_14d",
        "alta": "programar_taller_7d",
        "critica": "intervenir_48h",
    }

    detector_origen = {
        "wheel": "vision_wheel_profile",
        "brake": "vision_brake_thermal",
        "bogie": "vision_bogie_geometry",
        "pantograph": "vision_contact_strip",
    }

    insp_rows = []
    for i, row in enumerate(insp.itertuples(index=False), start=1):
        sev = severity.iloc[i - 1]
        insp_rows.append(
            (
                f"INSP{i:07d}",
                f"{row.fecha} 02:00:00",
                row.unidad_id,
                row.componente_id,
                row.familia_inspeccion,
                detector_origen.get(row.familia_inspeccion, "vision_generic"),
                sev,
                round(float(score.iloc[i - 1]), 3),
                int(defect_flag[i - 1]),
                round(float(confidence[i - 1]), 4),
                rec_map[sev],
            )
        )

    return pd.DataFrame(
        insp_rows,
        columns=[
            "inspeccion_id",
            "timestamp",
            "unidad_id",
            "componente_id",
            "familia_inspeccion",
            "detector_origen",
            "severidad_hallazgo",
            "score_defecto",
            "defecto_detectado",
            "confianza_deteccion",
            "recomendacion_inicial",
        ],
    )


def _generate_alertas_operativas(
    daily_state: pd.DataFrame,
    fallas: pd.DataFrame,
    inspecciones: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    d = daily_state.copy()

    cond_temp = (d["temperatura_operacion"] > 108) & (rng.random(len(d)) < 0.12)
    cond_vibr = (d["vibracion_proxy"] > 6.4) & (rng.random(len(d)) < 0.10)
    cond_wear = (d["desgaste_proxy"] > 92) & (rng.random(len(d)) < 0.16)

    sensor_alerts = d.loc[cond_temp | cond_vibr | cond_wear, ["fecha", "unidad_id", "componente_id", "degradacion_acumulada"]].copy()

    type_values = np.where(
        cond_wear.loc[sensor_alerts.index],
        "degradacion_acelerada",
        np.where(cond_vibr.loc[sensor_alerts.index], "anomalia_vibracional", "sobretemperatura"),
    )

    sev_values = np.where(
        sensor_alerts["degradacion_acumulada"] > 0.95,
        "critica",
        np.where(sensor_alerts["degradacion_acumulada"] > 0.80, "alta", "media"),
    )

    actions = {
        "media": "inspeccion_dirigida",
        "alta": "programar_intervencion_7d",
        "critica": "parada_controlada_48h",
    }

    rows = []
    idx = 1
    for i, row in enumerate(sensor_alerts.itertuples(index=False)):
        sev = sev_values[i]
        rows.append(
            (
                f"ALT{idx:08d}",
                f"{row.fecha} 13:00:00",
                row.unidad_id,
                row.componente_id,
                type_values[i],
                sev,
                "sensor",
                1,
                actions[sev],
                int(rng.choice([0, 1], p=[0.42, 0.58])),
            )
        )
        idx += 1

    insp_alerts = inspecciones[
        (inspecciones["defecto_detectado"] == 1) & (inspecciones["severidad_hallazgo"].isin(["alta", "critica"]))
    ]
    for row in insp_alerts.itertuples(index=False):
        sev = "critica" if row.severidad_hallazgo == "critica" else "alta"
        rows.append(
            (
                f"ALT{idx:08d}",
                row.timestamp,
                row.unidad_id,
                row.componente_id,
                "hallazgo_vision",
                sev,
                "inspeccion_automatica",
                1,
                "programar_intervencion_5d" if sev == "alta" else "intervenir_24h",
                int(rng.choice([0, 1], p=[0.35, 0.65])),
            )
        )
        idx += 1

    for row in fallas.itertuples(index=False):
        sev = "critica" if row.severidad_falla >= 4 else "alta"
        rows.append(
            (
                f"ALT{idx:08d}",
                f"{row.fecha_falla} 15:00:00",
                row.unidad_id,
                row.componente_id,
                "evento_falla",
                sev,
                "falla",
                0,
                "intervencion_inmediata",
                1,
            )
        )
        idx += 1

    return pd.DataFrame(
        rows,
        columns=[
            "alerta_id",
            "timestamp",
            "unidad_id",
            "componente_id",
            "tipo_alerta",
            "severidad",
            "trigger_origen",
            "alerta_temprana_flag",
            "accion_sugerida",
            "atendida_flag",
        ],
    )


def _generate_eventos_mantenimiento(
    componentes: pd.DataFrame,
    flotas: pd.DataFrame,
    unidades: pd.DataFrame,
    fallas: pd.DataFrame,
    alertas: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    unit_to_dep = unidades.set_index("unidad_id")["deposito_id"].to_dict()
    unit_to_flota = unidades.set_index("unidad_id")["flota_id"].to_dict()
    strategy_map = flotas.set_index("flota_id")["estrategia_mantenimiento_actual"].to_dict()

    rows = []
    idx = 1

    # Correctivos derivados de fallas
    for f in fallas.itertuples(index=False):
        lag_days = int(rng.integers(0, 3))
        start = pd.Timestamp(f.fecha_falla) + pd.Timedelta(days=lag_days)
        duration_h = float(np.clip(rng.gamma(1.6 + 0.65 * f.severidad_falla, 2.1), 1.8, 48))
        end = start + pd.Timedelta(hours=duration_h)
        rows.append(
            (
                f"MNT{idx:08d}",
                f.unidad_id,
                f.componente_id,
                unit_to_dep[f.unidad_id],
                start.isoformat(sep=" "),
                end.isoformat(sep=" "),
                "correctivo",
                f.modo_falla,
                1,
                0,
                0,
                round(duration_h, 3),
                round(float(duration_h * rng.uniform(85, 145)), 2),
                round(float((220 + 190 * f.severidad_falla) * rng.uniform(0.8, 1.5)), 2),
                rng.choice(["resuelto", "resuelto_con_recomendacion"], p=[0.72, 0.28]),
            )
        )
        idx += 1

    # Preventivos periódicos y CBM
    comp_meta = componentes.set_index("componente_id")
    early_alerts = alertas[(alertas["alerta_temprana_flag"] == 1) & (alertas["severidad"].isin(["alta", "critica"]))]

    for comp in componentes.itertuples(index=False):
        flota = unit_to_flota[comp.unidad_id]
        strategy = strategy_map[flota]

        install = pd.Timestamp(comp.fecha_instalacion)
        end_hist = pd.Timestamp(HISTORY_END)
        interval = int(np.clip(comp.vida_util_teorica_dias * rng.uniform(0.24, 0.34), 110, 320))

        if strategy in {"preventiva", "mixta"}:
            current = install + pd.Timedelta(days=interval)
            while current <= end_hist:
                duration_h = float(np.clip(rng.normal(4.8, 1.6), 1.5, 12))
                end = current + pd.Timedelta(hours=duration_h)
                rows.append(
                    (
                        f"MNT{idx:08d}",
                        comp.unidad_id,
                        comp.componente_id,
                        unit_to_dep[comp.unidad_id],
                        current.isoformat(sep=" "),
                        end.isoformat(sep=" "),
                        "preventivo",
                        "ciclo_planificado",
                        0,
                        0,
                        1,
                        round(duration_h, 3),
                        round(float(duration_h * rng.uniform(70, 120)), 2),
                        round(float(rng.uniform(120, 520)), 2),
                        rng.choice(["resuelto", "ajuste_realizado"], p=[0.83, 0.17]),
                    )
                )
                idx += 1
                current += pd.Timedelta(days=interval)

        if strategy in {"basada_condicion", "mixta"}:
            comp_alerts = early_alerts[early_alerts["componente_id"] == comp.componente_id]
            if not comp_alerts.empty:
                sampled = comp_alerts.sample(n=min(6, len(comp_alerts)), random_state=int(RANDOM_SEED))
                for al in sampled.itertuples(index=False):
                    start = pd.Timestamp(al.timestamp) + pd.Timedelta(days=int(rng.integers(1, 8)))
                    if start > end_hist:
                        continue
                    duration_h = float(np.clip(rng.normal(5.2, 1.4), 2.0, 13))
                    end = start + pd.Timedelta(hours=duration_h)
                    rows.append(
                        (
                            f"MNT{idx:08d}",
                            comp.unidad_id,
                            comp.componente_id,
                            unit_to_dep[comp.unidad_id],
                            start.isoformat(sep=" "),
                            end.isoformat(sep=" "),
                            "condicion",
                            "alerta_temprana",
                            0,
                            1,
                            1,
                            round(duration_h, 3),
                            round(float(duration_h * rng.uniform(75, 130)), 2),
                            round(float(rng.uniform(180, 760)), 2),
                            rng.choice(["resuelto", "degradacion_contenida"], p=[0.68, 0.32]),
                        )
                    )
                    idx += 1

    eventos = pd.DataFrame(
        rows,
        columns=[
            "mantenimiento_id",
            "unidad_id",
            "componente_id",
            "deposito_id",
            "fecha_inicio",
            "fecha_fin",
            "tipo_mantenimiento",
            "motivo_intervencion",
            "correctiva_flag",
            "basada_en_condicion_flag",
            "programada_flag",
            "horas_taller",
            "coste_mano_obra_proxy",
            "coste_material_proxy",
            "resultado_intervencion",
        ],
    )

    start_hist = pd.Timestamp(HISTORY_START)
    end_hist = pd.Timestamp(HISTORY_END)
    eventos["fecha_inicio"] = pd.to_datetime(eventos["fecha_inicio"], format="mixed", errors="coerce")
    eventos["fecha_fin"] = pd.to_datetime(eventos["fecha_fin"], format="mixed", errors="coerce")
    eventos = eventos[
        (eventos["fecha_inicio"] >= start_hist) & (eventos["fecha_inicio"] <= end_hist)
    ].copy()
    eventos["fecha_inicio"] = eventos["fecha_inicio"].dt.strftime("%Y-%m-%d %H:%M:%S")
    eventos["fecha_fin"] = eventos["fecha_fin"].dt.strftime("%Y-%m-%d %H:%M:%S")
    eventos = eventos.sort_values("fecha_inicio").reset_index(drop=True)
    return eventos


def _generate_intervenciones_programadas(
    eventos: pd.DataFrame,
    alertas: pd.DataFrame,
    unidades: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    unit_dep = unidades.set_index("unidad_id")["deposito_id"].to_dict()

    rows = []
    idx = 1
    for ev in eventos.itertuples(index=False):
        start = pd.Timestamp(ev.fecha_inicio)
        lead = int(rng.integers(1, 12)) if ev.programada_flag == 1 else int(rng.integers(0, 3))
        planned = start - pd.Timedelta(days=lead)

        if ev.correctiva_flag == 1:
            priority = int(np.clip(rng.normal(4.7, 0.4), 3, 5))
            status = "ejecutada"
        elif ev.basada_en_condicion_flag == 1:
            priority = int(np.clip(rng.normal(3.9, 0.8), 2, 5))
            status = rng.choice(["ejecutada", "diferida", "pendiente"], p=[0.64, 0.21, 0.15])
        else:
            priority = int(np.clip(rng.normal(3.0, 0.7), 1, 4))
            status = rng.choice(["ejecutada", "pendiente", "cancelada"], p=[0.72, 0.20, 0.08])

        impact = float(np.clip(priority * 16 + ev.horas_taller * 1.8 + rng.normal(0, 7), 8, 100))

        rows.append(
            (
                f"INT{idx:08d}",
                ev.unidad_id,
                ev.componente_id,
                ev.deposito_id,
                planned.date().isoformat(),
                priority,
                status,
                int(rng.choice([0, 1], p=[0.24, 0.76])),
                round(impact, 3),
            )
        )
        idx += 1

    # Intervenciones futuras pendientes por alertas no atendidas
    late_alerts = alertas[(alertas["atendida_flag"] == 0) & (alertas["alerta_temprana_flag"] == 1)].copy()
    late_alerts = late_alerts.sort_values("timestamp").tail(350)
    for al in late_alerts.itertuples(index=False):
        planned = pd.Timestamp(al.timestamp) + pd.Timedelta(days=int(rng.integers(2, 20)))
        rows.append(
            (
                f"INT{idx:08d}",
                al.unidad_id,
                al.componente_id,
                unit_dep[al.unidad_id],
                planned.date().isoformat(),
                5 if al.severidad == "critica" else 4,
                rng.choice(["pendiente", "diferida"], p=[0.58, 0.42]),
                int(rng.choice([0, 1], p=[0.36, 0.64])),
                round(float(np.clip(rng.normal(78, 14), 35, 100)), 3),
            )
        )
        idx += 1

    intervenciones = pd.DataFrame(
        rows,
        columns=[
            "intervencion_id",
            "unidad_id",
            "componente_id",
            "deposito_id",
            "fecha_programada",
            "prioridad_planificada",
            "estado_intervencion",
            "ventana_operativa_disponible",
            "impacto_si_no_se_ejecuta",
        ],
    )
    start_hist = pd.Timestamp(HISTORY_START)
    end_hist = pd.Timestamp(HISTORY_END)
    intervenciones["fecha_programada"] = pd.to_datetime(intervenciones["fecha_programada"], errors="coerce")
    intervenciones = intervenciones[
        (intervenciones["fecha_programada"] >= start_hist)
        & (intervenciones["fecha_programada"] <= end_hist)
    ].copy()
    intervenciones["fecha_programada"] = intervenciones["fecha_programada"].dt.date.astype(str)
    return intervenciones


def _generate_disponibilidad_y_asignacion(
    unit_day_context: pd.DataFrame,
    unidades: pd.DataFrame,
    flotas: pd.DataFrame,
    fallas: pd.DataFrame,
    eventos: pd.DataFrame,
    rng: np.random.Generator,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    base = unit_day_context[[
        "fecha",
        "unidad_id",
        "linea_servicio",
        "horas_planificadas",
        "servicio_planificado",
        "reserva_flag",
        "nivel_congestion_operativa_proxy",
    ]].copy()

    unit_to_flota = unidades.set_index("unidad_id")["flota_id"].to_dict()
    base["flota_id"] = base["unidad_id"].map(unit_to_flota)

    falla_agg = (
        fallas.groupby(["fecha_falla", "unidad_id"], as_index=False)
        .agg(
            downtime_falla=("tiempo_fuera_servicio_horas", "sum"),
            severidad_media_falla=("severidad_falla", "mean"),
            n_fallas=("falla_id", "count"),
        )
        .rename(columns={"fecha_falla": "fecha"})
    )

    eventos_aux = eventos.copy()
    eventos_aux["fecha_inicio"] = pd.to_datetime(eventos_aux["fecha_inicio"])
    eventos_aux["fecha"] = eventos_aux["fecha_inicio"].dt.date.astype(str)

    mnt_agg = (
        eventos_aux.groupby(["fecha", "unidad_id"], as_index=False)
        .agg(
            downtime_mnt=("horas_taller", "sum"),
            n_mnt=("mantenimiento_id", "count"),
            pct_correctivo=("correctiva_flag", "mean"),
        )
    )

    df = base.merge(falla_agg, on=["fecha", "unidad_id"], how="left")
    df = df.merge(mnt_agg, on=["fecha", "unidad_id"], how="left")
    df[["downtime_falla", "severidad_media_falla", "n_fallas", "downtime_mnt", "n_mnt", "pct_correctivo"]] = df[
        ["downtime_falla", "severidad_media_falla", "n_fallas", "downtime_mnt", "n_mnt", "pct_correctivo"]
    ].fillna(0)

    congestion = df["nivel_congestion_operativa_proxy"].astype(float)
    plan = df["horas_planificadas"].astype(float)
    no_disp = np.clip(
        df["downtime_falla"].astype(float)
        + (df["downtime_mnt"].astype(float) * 0.65)
        + np.maximum(0, congestion - 0.75) * 1.8
        + rng.normal(0.12, 0.35, len(df)),
        0,
        None,
    )

    no_disp = np.minimum(no_disp, plan)
    disp = np.clip(plan - no_disp, 0, None)

    cancelaciones = np.clip(np.round(np.maximum(0, no_disp - 1.3) * (0.7 + congestion)), 0, None)
    punctuality = np.clip((no_disp / np.maximum(plan, 0.1)) * 55 + congestion * 34 + rng.normal(2, 3, len(df)), 0, 100)

    reasons = np.where(
        df["downtime_falla"] > df["downtime_mnt"],
        "falla_tecnica",
        np.where(df["downtime_mnt"] > 0, "mantenimiento", "operacion_normal"),
    )

    disponibilidad = pd.DataFrame(
        {
            "fecha": df["fecha"],
            "unidad_id": df["unidad_id"],
            "flota_id": df["flota_id"],
            "linea_servicio": df["linea_servicio"],
            "horas_planificadas": np.round(plan, 3),
            "horas_disponibles": np.round(disp, 3),
            "horas_no_disponibles": np.round(no_disp, 3),
            "motivo_no_disponibilidad": reasons,
            "cancelaciones_proxy": cancelaciones.astype(int),
            "puntualidad_impactada_proxy": np.round(punctuality, 3),
        }
    )

    servicio_realizado = np.maximum(0, df["servicio_planificado"] - cancelaciones.astype(int) - (no_disp > 4.0).astype(int))
    sustitucion = ((servicio_realizado < (df["servicio_planificado"] * 0.84)) & (df["reserva_flag"] == 0)).astype(int)

    asignacion = pd.DataFrame(
        {
            "fecha": df["fecha"],
            "unidad_id": df["unidad_id"],
            "linea_servicio": df["linea_servicio"],
            "servicio_planificado": df["servicio_planificado"].astype(int),
            "servicio_realizado": servicio_realizado.astype(int),
            "reserva_flag": df["reserva_flag"].astype(int),
            "sustitucion_requerida_flag": sustitucion.astype(int),
        }
    )

    return disponibilidad, asignacion


def _generate_backlog_mantenimiento(
    intervenciones: pd.DataFrame,
    fechas: pd.DatetimeIndex,
    rng: np.random.Generator,
) -> pd.DataFrame:
    pending = intervenciones[intervenciones["estado_intervencion"].isin(["pendiente", "diferida"])].copy()
    pending["fecha_programada_dt"] = pd.to_datetime(pending["fecha_programada"])

    snapshot_dates = pd.date_range(start=fechas.min(), end=fechas.max(), freq="7D")
    rows = []

    for snap in snapshot_dates:
        active = pending[pending["fecha_programada_dt"] <= snap]
        for row in active.itertuples(index=False):
            age = int((snap - row.fecha_programada_dt).days)
            severidad = "critica" if row.prioridad_planificada >= 5 else ("alta" if row.prioridad_planificada >= 4 else "media")
            riesgo = np.clip(row.impacto_si_no_se_ejecuta + age * 0.52 + rng.normal(0, 3), 0, 180)
            pendencia = "correctiva_pendiente" if row.prioridad_planificada >= 5 else "preventiva_pendiente"
            rows.append(
                (
                    snap.date().isoformat(),
                    row.deposito_id,
                    row.unidad_id,
                    row.componente_id,
                    pendencia,
                    age,
                    severidad,
                    round(float(riesgo), 3),
                )
            )

    return pd.DataFrame(
        rows,
        columns=[
            "fecha",
            "deposito_id",
            "unidad_id",
            "componente_id",
            "tipo_pendencia",
            "antiguedad_backlog_dias",
            "severidad_pendiente",
            "riesgo_acumulado",
        ],
    )


def _generate_escenarios_mantenimiento(
    parametros_contexto: pd.DataFrame,
    backlog: pd.DataFrame,
    fechas: pd.DatetimeIndex,
    rng: np.random.Generator,
) -> pd.DataFrame:
    intensity_daily = parametros_contexto.groupby("fecha", as_index=False)["intensidad_servicio"].mean()
    backlog_daily = backlog.groupby("fecha", as_index=False)["riesgo_acumulado"].mean()

    base = pd.DataFrame({"fecha": fechas.date.astype(str)})
    base = base.merge(intensity_daily, on="fecha", how="left").rename(columns={"intensidad_servicio": "intensity"})
    base = base.merge(backlog_daily, on="fecha", how="left").rename(columns={"riesgo_acumulado": "backlog_risk"})
    base[["intensity", "backlog_risk"]] = base[["intensity", "backlog_risk"]].ffill().fillna(0)

    scenarios = []
    cfg = {
        "reactivo": (1.08, 1.20, 0.82, 1.18),
        "preventivo_rigido": (1.00, 0.92, 0.90, 1.02),
        "basado_en_condicion": (0.97, 0.78, 1.08, 0.93),
    }

    for scenario, (f_int, f_back, f_res, f_cost) in cfg.items():
        for row in base.itertuples(index=False):
            intensity = np.clip(row.intensity * f_int + rng.normal(0, 0.02), 0.5, 1.7)
            backlog_idx = np.clip((row.backlog_risk / 100) * f_back + rng.normal(0, 0.03), 0.05, 2.3)
            resources = np.clip((0.95 - 0.32 * backlog_idx) * f_res + rng.normal(0, 0.03), 0.1, 1.4)
            cost = np.clip((0.68 * intensity + 0.42 * backlog_idx) * f_cost + rng.normal(0, 0.03), 0.2, 2.5)
            scenarios.append(
                (
                    row.fecha,
                    scenario,
                    round(float(intensity), 4),
                    round(float(backlog_idx), 4),
                    round(float(resources), 4),
                    round(float(cost), 4),
                )
            )

    return pd.DataFrame(
        scenarios,
        columns=[
            "fecha",
            "escenario",
            "intensidad_operacion_indice",
            "tension_backlog_indice",
            "disponibilidad_recursos_indice",
            "presion_coste_indice",
        ],
    )


def _build_plausibility_validations(
    tables: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    checks = []

    fechas = pd.to_datetime(tables["parametros_operativos_contexto"]["fecha"])
    history_days = int((fechas.max() - fechas.min()).days + 1)
    checks.append(("historial_minimo_24_meses", history_days >= 730, history_days, ">=730"))

    comp_keys = set(tables["componentes_criticos"]["componente_id"])
    unit_keys = set(tables["unidades"]["unidad_id"])
    dep_keys = set(tables["depositos"]["deposito_id"])

    checks.append((
        "sensores_componentes_fk_componente",
        tables["sensores_componentes"]["componente_id"].isin(comp_keys).all(),
        int((~tables["sensores_componentes"]["componente_id"].isin(comp_keys)).sum()),
        "0_orphans",
    ))
    checks.append((
        "fallas_fk_unidad",
        tables["fallas_historicas"]["unidad_id"].isin(unit_keys).all(),
        int((~tables["fallas_historicas"]["unidad_id"].isin(unit_keys)).sum()),
        "0_orphans",
    ))
    checks.append((
        "mantenimiento_fk_deposito",
        tables["eventos_mantenimiento"]["deposito_id"].isin(dep_keys).all(),
        int((~tables["eventos_mantenimiento"]["deposito_id"].isin(dep_keys)).sum()),
        "0_orphans",
    ))

    disp = tables["disponibilidad_servicio"]
    reconciliation_error = (disp["horas_planificadas"] - (disp["horas_disponibles"] + disp["horas_no_disponibles"])).abs().mean()
    checks.append(("disponibilidad_reconciliada", reconciliation_error < 0.01, round(float(reconciliation_error), 6), "<0.01"))

    required_families = {"wheel", "brake", "bogie", "pantograph"}
    families_present = set(tables["inspecciones_automaticas"]["familia_inspeccion"].unique())
    checks.append((
        "familias_inspeccion_requeridas",
        required_families.issubset(families_present),
        ",".join(sorted(families_present)),
        "wheel,brake,bogie,pantograph",
    ))

    temp_range_ok = tables["sensores_componentes"]["temperatura_operacion"].between(-20, 170).all()
    checks.append(("temperatura_sensor_rango", temp_range_ok, int((~tables["sensores_componentes"]["temperatura_operacion"].between(-20, 170)).sum()), "0_outliers"))

    fail_downtime_positive = (tables["fallas_historicas"]["tiempo_fuera_servicio_horas"] > 0).all()
    checks.append(("downtime_fallas_positivo", fail_downtime_positive, int((tables["fallas_historicas"]["tiempo_fuera_servicio_horas"] <= 0).sum()), "0_nonpositive"))

    return pd.DataFrame(checks, columns=["check", "aprobado", "valor_observado", "umbral"])


def _build_cardinality_summary(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for name, df in tables.items():
        date_col = None
        for candidate in ["fecha", "timestamp", "fecha_falla", "fecha_programada", "fecha_inicio"]:
            if candidate in df.columns:
                date_col = candidate
                break

        if date_col is not None:
            dt = pd.to_datetime(df[date_col], errors="coerce")
            min_date = dt.min()
            max_date = dt.max()
        else:
            min_date = pd.NaT
            max_date = pd.NaT

        rows.append(
            {
                "tabla": name,
                "n_filas": int(len(df)),
                "n_columnas": int(df.shape[1]),
                "nulos_pct_promedio": round(float(df.isna().mean().mean() * 100), 4),
                "fecha_min": "" if pd.isna(min_date) else str(min_date.date()),
                "fecha_max": "" if pd.isna(max_date) else str(max_date.date()),
            }
        )

    return pd.DataFrame(rows).sort_values("tabla")


def _write_generation_logic_summary(
    tables: Dict[str, pd.DataFrame],
    validations: pd.DataFrame,
    summary: pd.DataFrame,
) -> None:
    lines = [
        "# Resumen de Lógica Sintética",
        "",
        "## Diseño de simulación",
        "- Historial completo de 24 meses (2024-01-01 a 2025-12-31).",
        "- Múltiples flotas, líneas y depósitos con parámetros de operación heterogéneos.",
        "- Degradación acumulativa por componente con aceleración por carga, congestión y ambiente.",
        "- Sensores con ruido realista y comportamiento dependiente de desgaste.",
        "- Inspección automática por familias wheel, brake, bogie y pantograph.",
        "- Fallas probabilísticas con modo de falla plausible y bandera repetitiva.",
        "- Mezcla de mantenimiento correctivo, preventivo y basado en condición.",
        "- Disponibilidad y asignación de servicio afectadas por fallas e intervenciones.",
        "- Backlog y escenarios sintetizados para análisis de riesgo y capacidad.",
        "",
        "## Cardinalidades clave",
    ]

    for row in summary.sort_values("n_filas", ascending=False).head(8).itertuples(index=False):
        lines.append(f"- {row.tabla}: {row.n_filas:,} filas | {row.n_columnas} columnas")

    lines.extend(["", "## Estado de validaciones"]) 
    for row in validations.itertuples(index=False):
        state = "OK" if bool(row.aprobado) else "FAIL"
        lines.append(f"- {row.check}: {state} (observado={row.valor_observado}, umbral={row.umbral})")

    (DATA_RAW_DIR / "resumen_generacion_sintetica.md").write_text("\n".join(lines), encoding="utf-8")


def _save_required_tables(data: SyntheticRailwayData) -> Dict[str, pd.DataFrame]:
    tables = {
        "flotas": data.flotas,
        "unidades": data.unidades,
        "depositos": data.depositos,
        "componentes_criticos": data.componentes_criticos,
        "sensores_componentes": data.sensores_componentes,
        "inspecciones_automaticas": data.inspecciones_automaticas,
        "eventos_mantenimiento": data.eventos_mantenimiento,
        "fallas_historicas": data.fallas_historicas,
        "alertas_operativas": data.alertas_operativas,
        "intervenciones_programadas": data.intervenciones_programadas,
        "disponibilidad_servicio": data.disponibilidad_servicio,
        "asignacion_servicio": data.asignacion_servicio,
        "backlog_mantenimiento": data.backlog_mantenimiento,
        "parametros_operativos_contexto": data.parametros_operativos_contexto,
        "escenarios_mantenimiento": data.escenarios_mantenimiento,
    }

    for name, df in tables.items():
        df.to_csv(DATA_RAW_DIR / f"{name}.csv", index=False)

    return tables


def generate_synthetic_data(seed: int = RANDOM_SEED) -> SyntheticRailwayData:
    rng = np.random.default_rng(seed)
    fechas = pd.date_range(HISTORY_START, HISTORY_END, freq="D")

    flotas = _build_flotas()
    depositos = _build_depositos()
    lineas = _build_lineas()

    unidades = _generate_unidades(flotas, depositos, lineas, rng)
    componentes = _generate_componentes_criticos(unidades, rng)
    parametros = _generate_parametros_operativos_contexto(lineas, fechas, rng)
    unit_day_context = _build_unit_day_context(unidades, flotas, parametros, rng)

    daily_state, sensores = _simulate_component_states_and_sensors(componentes, unit_day_context, rng)
    fallas = _generate_fallas_historicas(daily_state, rng)
    inspecciones = _generate_inspecciones_automaticas(daily_state, rng)
    alertas = _generate_alertas_operativas(daily_state, fallas, inspecciones, rng)
    eventos = _generate_eventos_mantenimiento(componentes, flotas, unidades, fallas, alertas, rng)
    intervenciones = _generate_intervenciones_programadas(eventos, alertas, unidades, rng)
    disponibilidad, asignacion = _generate_disponibilidad_y_asignacion(unit_day_context, unidades, flotas, fallas, eventos, rng)
    backlog = _generate_backlog_mantenimiento(intervenciones, fechas, rng)
    escenarios = _generate_escenarios_mantenimiento(parametros, backlog, fechas, rng)

    data = SyntheticRailwayData(
        flotas=flotas,
        unidades=unidades,
        depositos=depositos,
        componentes_criticos=componentes[
            [
                "componente_id",
                "unidad_id",
                "sistema_principal",
                "subsistema",
                "tipo_componente",
                "fabricante_proxy",
                "fecha_instalacion",
                "edad_componente_dias",
                "ciclos_acumulados",
                "criticidad_componente",
                "vida_util_teorica_dias",
                "vida_util_teorica_ciclos",
            ]
        ],
        sensores_componentes=sensores,
        inspecciones_automaticas=inspecciones,
        eventos_mantenimiento=eventos,
        fallas_historicas=fallas,
        alertas_operativas=alertas,
        intervenciones_programadas=intervenciones,
        disponibilidad_servicio=disponibilidad,
        asignacion_servicio=asignacion,
        backlog_mantenimiento=backlog,
        parametros_operativos_contexto=parametros,
        escenarios_mantenimiento=escenarios,
    )

    tables = _save_required_tables(data)
    validations = _build_plausibility_validations(tables)
    summary = _build_cardinality_summary(tables)

    validations.to_csv(DATA_RAW_DIR / "validaciones_plausibilidad.csv", index=False)
    summary.to_csv(DATA_RAW_DIR / "resumen_dimensiones_cardinalidades.csv", index=False)
    _write_generation_logic_summary(tables, validations, summary)

    return data


if __name__ == "__main__":
    generate_synthetic_data()
